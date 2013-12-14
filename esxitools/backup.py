import os
import datetime
from glob import glob
import re
from tendo import singleton
import paramiko
from scp import SCPClient
from ftplib import FTP
from string import Template
from tempfile import mkstemp
import logging
import io

import utils

log_stream = io.StringIO()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sh = logging.StreamHandler(log_stream)
sh.setLevel(logging.DEBUG)
sh.setFormatter(logging.Formatter(u'%(asctime)s\t%(levelname)s\t%(message)s'))
logger.addHandler(sh)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

try:
    import settings
except ImportError:
    logger.error(u'No settings.py file found!')
    import sys
    sys.exit(1)

def is_time_in_window(t, ranges):
    for ts, te in ranges:
        if ts <= t <= te:
            return True
    return False

def get_current_time():
    import time
    now = time.localtime()
    return datetime.time(now.tm_hour, now.tm_min, now.tm_sec)

class BackupProfile(object):
    _no_such_file_or_dir_re = re.compile(u'No such file or directory')
    _backup_archive_re = re.compile(u'(?P<vmname>.+)\-'
        '(?P<ts>\d{4}\-\d{2}\-\d{2}\_\d{2}\-\d{2}\-\d{2})\.tar\.gz')
    _t = None
    _chan = None
    
    @classmethod
    def _get_current_time(cls):
        return datetime.datetime.now()
    
    @classmethod
    def _apply_template(cls, tmpl_file_path, tmpl_params, out_file_path=None):
        """
        Applies template-parameters to template-file.
        Creates an output file with applied template.
        If `out_file_path` not specified, a temp file will be used.
        """
        # Read the content of the file as a template string
        with open(tmpl_file_path, 'r') as tmpl_file:
            tmpl_str = Template(tmpl_file.read())
        # Apply the template and save to the output file
        out_string = tmpl_str.safe_substitute(tmpl_params)
        if not out_file_path:
            f, out_file_path = mkstemp(text=True)
            os.close(f)
        with io.open(out_file_path, 'w', newline='\n') as f:
            f.write(out_string)
        return out_file_path
    
    def __init__(self, profile_dict):
        self.__dict__.update(profile_dict)
    
    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        self._close_ssh_transport()
    
    def _get_ssh_transport(self):
        if self._t:
            return self._t
        self._t = paramiko.Transport((self.host_ip, self.ssh_port))
        self._t.start_client()
        self._t.auth_password(self.ssh_user, self.ssh_password)
        return self._t
    
    def _close_ssh_transport(self):
        self._close_ssh_session()
        if self._t:
            self._t.close()
            self._t = None
    
    def _get_ssh_session(self):
        # if self._chan and not self._chan.closed:
            # print 'pre', self._chan
            # return self._chan
        self._chan = self._get_ssh_transport().open_session()
        self._chan.set_combine_stderr(True)
        return self._chan
    
    def _close_ssh_session(self):
        if self._chan:
            self._chan.close()
            self._chan = None
    
    def _run_ssh_command(self, cmd):
        # Open an SSH session and execute the command
        chan = self._get_ssh_session()
        chan.exec_command('%s ; echo exit_code=$?' % (cmd))
        stdout = ''
        x = chan.recv(1024)
        while x:
            stdout += x
            x = chan.recv(1024)
        output = stdout.strip().split('\n')
        exit_code = re.match('exit_code\=(\-?\d+)', output[-1]).group(1)
        if not '0' == exit_code:
            logger.debug(u'SSH command "%s" failed with output:\n%s' %
                         (cmd, '\n'.join(output)))
            raise RuntimeWarning(u'Remote command failed with code %s' %
                                 (exit_code))
        return '\n'.join(output[:-1])
    
    def _get_vm_config(self, vmname, config):
        vm_dict = self.backup_vms[vmname]
        if config in vm_dict:
            return vm_dict[config]
        return self.default_vm_config[config]
    
    def _list_backup_archives(self):
        glob_str = os.path.join(self.backups_archive_dir, u'*.tar.gz')
        return glob(glob_str)
    
    def _list_backup_archives_for_vm(self, vmname):
        glob_str = os.path.join(self.backups_archive_dir,
                                u'%s-*.tar.gz' % (vmname))
        return glob(glob_str)
    
    def get_latest_archives(self):
        """
        Returns dictionary of existing archives in `backup_archive_dir`,
        with VM names as keys and the latest available backup timestamp
        as value.
        """
        res = dict()
        for archive_path in self._list_backup_archives():
            _, archive = os.path.split(archive_path)
            m = re.match(u'(?P<vmname>.+)\-'
                '(?P<ts>\d{4}\-\d{2}\-\d{2}\_\d{2}\-\d{2}\-\d{2})\.tar\.gz',
                archive)
            if m:
                vmname = m.groupdict()[u'vmname']
                ts = datetime.datetime.strptime(m.groupdict()[u'ts'],
                                                '%Y-%m-%d_%H-%M-%S')
                if vmname in res:
                    if ts > res[vmname]:
                        res[vmname] = ts
                else:
                    res[vmname] = ts
        return res
    
    def is_vm_backup_overdue(self, vmname, ts):
        "Returns True if `vmname` backup from `ts` is older than period"
        time_since_last_backup = self._get_current_time() - ts
        if not vmname in self.backup_vms:
            logger.warning(u'VM "%s" not in profile, but archive found' %
                            (vmname))
            return False
        period = self._get_vm_config(vmname, u'period')
        assert type(period) == datetime.timedelta
        return time_since_last_backup >= period
    
    def get_next_vm_to_backup(self):
        """
        """
        # First priority - VMs with no existing archives
        for vmname in self.backup_vms.keys():
            if not self._list_backup_archives_for_vm(vmname):
                logger.debug(u'VM "%s" is ready next (no existing archives)' %
                             vmname)
                return vmname
        # Second priority - the VM with the oldest archive that is overdue
        ret_vm = None
        ret_vm_last_backup = None
        for vmname, ts in self.get_latest_archives().iteritems():
            if self.is_vm_backup_overdue(vmname, ts):
                logger.debug(u'VM "%s" backup is overdue' % (vmname))
                if ret_vm_last_backup:
                    if ts < ret_vm_last_backup:
                        ret_vm = vmname
                        ret_vm_last_backup = ts
                else:
                    ret_vm = vmname
                    ret_vm_last_backup = ts
        return ret_vm
    
    def _upload_file(self, local_source, remote_destination):
        scp = SCPClient(self._get_ssh_transport())
        scp.put(local_source, remote_destination)
    
    def _set_remote_chmod(self, remote_file):
        return self._run_ssh_command(u'chmod +x %s' % (remote_file))
    
    def _remove_remote_file(self, remote_file):
        self._run_ssh_command('rm %s' % (remote_file))
    
    def _remove_local_file(self, file):
        os.remove(file)
    
    def _parse_ghettovcb_output(self, raw_output):
        ret_dict = {u'WARNINGS': list()}
        info_prefix = u'\d{4}\-\d{2}\-\d{2} \d{2}\:\d{2}\:\d{2} \-\- info\:'
        config_matcher = re.compile(
            u'%s CONFIG \- (?P<key>\w+) \= (?P<val>.+)' % (info_prefix))
        warn_matcher = re.compile(u'%s WARN\: (?P<msg>.+)' % (info_prefix))
        duration_matcher = re.compile(
            u'%s Backup Duration\: (?P<time>.+)' % (info_prefix))
        final_status_matcher = re.compile(
            u'%s \#{6} Final status\: (?P<status>.+) \#{6}' % (info_prefix))
        for raw_line in raw_output.split(u'\n'):
            config = config_matcher.match(raw_line)
            if config:
                ret_dict[config.groupdict()[u'key']] =  \
                    config.groupdict()[u'val']
                continue
            warning = warn_matcher.match(raw_line)
            if warning:
                ret_dict[u'WARNINGS'].append(warning.groupdict()[u'msg'])
                continue
            duration = duration_matcher.match(raw_line)
            if duration:
                ret_dict[u'BACKUP_DURATION'] = duration.groupdict()[u'time']
                continue
            final_status = final_status_matcher.match(raw_line)
            if final_status:
                status = final_status.groupdict()[u'status']
                ret_dict[u'FINAL_STATUS'] = u'All VMs backed up OK!' == status
                continue
        return ret_dict
    
    def _run_remote_backup(self, vmname):
        "Run ghettovcb script to backup the specified VM"
        # Generate ghettovcb script from template
        local_script = self._apply_template(
            self.ghettovcb_script_template,
            {u'RemoteBackupDir': self.remote_backup_dir}
        )
        # Upload ghettovcb script to host and make it executable
        remote_script = '/'.join((self.remote_workdir, 'ghettovcb.sh'))
        self._upload_file(local_script, remote_script)
        self._set_remote_chmod(remote_script)
        # cleanup local temp
        self._remove_local_file(local_script)
        # Run ghettovcb script for the requested vm-name
        backup_cmd = '%s -m %s' % (remote_script, vmname)
        cmd_result = self._run_ssh_command(backup_cmd)
        self._remove_remote_file(remote_script)
        # Parse the output and return the result
        return self._parse_ghettovcb_output(cmd_result)
    
    def _archive_remote_backup(self, vmname, backup_dir):
        "Tar's and GZip's the backup dir, returning full path of the archive"
        remote_workdir = u'/'.join((self.remote_backup_dir, vmname))
        remote_archive = u'%s.tar.gz' % (backup_dir)
        tar_cmd = u'cd "%s"; tar -cz -f "%s" "%s"' %    \
                    (remote_workdir, remote_archive, backup_dir)
        tar_output = self._run_ssh_command(tar_cmd)
        if self._no_such_file_or_dir_re.search(tar_output):
            raise RuntimeError(u'Tar command failed:\n%s' % (tar_output))
        return '/'.join((remote_workdir, remote_archive))
    
    def _download_archive(self, remote_path):
        """
        Downloads a remote file at `remote_path` via FTP to
        `self.backups_archive_dir` using same file name,
        returning the total time it took (in seconds).
        """
        from time import time
        ts  = time()
        _, remote_filename = os.path.split(remote_path)
        dest_path = os.path.join(self.backups_archive_dir, remote_filename)
        ftp = FTP(self.host_ip)
        ftp.login(self.ftp_user, self.ftp_password)
        with open(dest_path, 'wb') as dest_file:
            ftp.retrbinary(u'RETR %s' % (remote_path), dest_file.write)
        return time() - ts
    
    def backup_vm(self, vmname):
        ghettovcb_output = self._run_remote_backup(vmname)
        logger.info(u'ghettovcb output:\n%s' % (
            u'\n'.join(
            [u'\t%s: %s' % (k,v)
             for k,v in ghettovcb_output.iteritems()])))
        if not ghettovcb_output[u'FINAL_STATUS']:
            # Something failed
            return False
        backup_name = ghettovcb_output[u'VM_BACKUP_DIR_NAMING_CONVENTION']
        backup_dir = u'%s-%s' % (vmname, backup_name)
        remote_archive = self._archive_remote_backup(vmname, backup_dir)
        download_time = self._download_archive(remote_archive)
        logger.info(u'Backup archive "%s" downloaded to "%s" in %f seconds.' %
                    (remote_archive, self.backups_archive_dir, download_time))
        self._remove_remote_file(remote_archive)
        logger.info(u'Cleaned up archive from remote host')
    
    def trim_backup_archives(self):
        for vmname in self.backup_vms.keys():
            vm_archives = self._list_backup_archives_for_vm(vmname)
            rot_count = self._get_vm_config(vmname, u'rotation_count')
            for archive_to_delete in sorted(vm_archives)[:-rot_count]:
                logger.info(u'Deleting archive "%s"' %
                            (archive_to_delete))
                self._remove_local_file(archive_to_delete)

def backup(**kwargs):
    # Avoid multiple instances of backup program
    me = singleton.SingleInstance(flavor_id=u'esxi-backup')
    # Obtain profile configuration
    if not u'profile_name' in kwargs:
        raise RuntimeError(u'Missing profile_name argument')
    profile_name = kwargs[u'profile_name']
    if not profile_name in settings.ESXI_BACKUP_PROFILES:
        raise RuntimeError(u'No such profile "%s"' % profile_name)
    profile = settings.ESXI_BACKUP_PROFILES[profile_name]
    logger.info(u'Running backup profile "%s"' % (profile_name))
    # Check if profile is currently active
    t = get_current_time()
    if not is_time_in_window(t, profile['backup_times']):
        logger.debug(u'Out of time range. Skipping backup run for profile.')
        return True
    with BackupProfile(profile) as bp:
        next_vm = bp.get_next_vm_to_backup()
        if next_vm:
            logger.info(u'Running backup for VM "%s"' % (next_vm))
            bp.backup_vm(next_vm)
            bp.trim_backup_archives()
            if bp.email_report:
                utils.send_email(
                    bp.gmail_user, bp.gmail_pwd, bp.from_field, bp.recipients,
                    u'BACKUP OK %s' % (next_vm), log_stream.getvalue())
        else:
            logger.info(u'No next VM to backup - Nothing to do.')
    return True
