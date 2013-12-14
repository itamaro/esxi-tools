import unittest
from mock import Mock, call, patch, MagicMock
from contextlib import nested
from datetime import timedelta
import os

# Module under test
import backup

def test_time_ranges():
    from datetime import time
    ranges = ( (time(23,00,00), time.max),
               (time.min, time(04,00,00)) )
    should_be_true = [
        time(23,0,0),
        time(23,30,0),
        time(23,59,59),
        time(0,0,0),
        time(0,0,1),
        time(2,30,0),
        time(3,59,59),
        time(4,0,0),
    ]
    should_be_false = [
        time(22,59,59),
        time(22,0,0),
        time(4,0,1),
        time(11,0,0),
        time(12,0,0),
    ]
    for t in should_be_true:
        assert backup.is_time_in_window(t, ranges) is True
    for t in should_be_false:
        assert backup.is_time_in_window(t, ranges) is False

class BackupTests(unittest.TestCase):
    def test_missing_prof_argument(self):
        "Check that an error is raised if profile_name is missing"
        with self.assertRaises(RuntimeError) as cm:
            backup.backup()
        self.assertEquals(cm.exception.message,
                    u'Missing profile_name argument')
        
    def test_non_existing_profile(self):
        "Check that an error is raised if profile missing from settings"
        with self.assertRaises(RuntimeError) as cm:
            backup.backup(profile_name=u'no-such-profile')
        self.assertEquals(cm.exception.message,
                    u'No such profile "no-such-profile"')

    def test_time_out_of_range(self):
        pass

ghettovcb_output_no_vm = """
Logging output to "/tmp/ghettoVCB-2013-12-03_16-30-19-1752530.log" ...
2013-12-03 16:30:19 -- info: ============================== ghettoVCB LOG START ==============================

2013-12-03 16:30:19 -- info: CONFIG - VERSION = 2013_01_11_0
2013-12-03 16:30:19 -- info: CONFIG - GHETTOVCB_PID = 1752530
2013-12-03 16:30:19 -- info: CONFIG - VM_BACKUP_VOLUME = /vmfs/volumes/Backup-LUN/BackupsDir
2013-12-03 16:30:19 -- info: CONFIG - VM_BACKUP_ROTATION_COUNT = 2
2013-12-03 16:30:19 -- info: CONFIG - VM_BACKUP_DIR_NAMING_CONVENTION = 2013-12-03_16-30-19
2013-12-03 16:30:19 -- info: CONFIG - DISK_BACKUP_FORMAT = thin
2013-12-03 16:30:19 -- info: CONFIG - POWER_VM_DOWN_BEFORE_BACKUP = 0
2013-12-03 16:30:19 -- info: CONFIG - ENABLE_HARD_POWER_OFF = 0
2013-12-03 16:30:19 -- info: CONFIG - ITER_TO_WAIT_SHUTDOWN = 3
2013-12-03 16:30:19 -- info: CONFIG - POWER_DOWN_TIMEOUT = 5
2013-12-03 16:30:19 -- info: CONFIG - SNAPSHOT_TIMEOUT = 15
2013-12-03 16:30:19 -- info: CONFIG - LOG_LEVEL = info
2013-12-03 16:30:19 -- info: CONFIG - BACKUP_LOG_OUTPUT = /tmp/ghettoVCB-2013-12-03_16-30-19-1752530.log
2013-12-03 16:30:19 -- info: CONFIG - ENABLE_COMPRESSION = 0
2013-12-03 16:30:19 -- info: CONFIG - VM_SNAPSHOT_MEMORY = 0
2013-12-03 16:30:19 -- info: CONFIG - VM_SNAPSHOT_QUIESCE = 0
2013-12-03 16:30:19 -- info: CONFIG - ALLOW_VMS_WITH_SNAPSHOTS_TO_BE_BACKEDUP = 0
2013-12-03 16:30:19 -- info: CONFIG - VMDK_FILES_TO_BACKUP = all
2013-12-03 16:30:19 -- info: CONFIG - VM_SHUTDOWN_ORDER = 
2013-12-03 16:30:19 -- info: CONFIG - VM_STARTUP_ORDER = 
2013-12-03 16:30:19 -- info: CONFIG - EMAIL_LOG = 0
2013-12-03 16:30:19 -- info: 
2013-12-03 16:30:20 -- info: ERROR: failed to locate and extract VM_ID for NoSuchVM!

2013-12-03 16:30:20 -- info: ###### Final status: ERROR: All VMs failed! ######

2013-12-03 16:30:20 -- info: ============================== ghettoVCB LOG END ================================
"""

ghettovcb_output_vm_with_independent_vmdk_poweroff = """
Logging output to "/tmp/ghettoVCB-2013-12-04_07-57-55-$.log" ...
2013-12-04 07:57:56 -- info: ============================== ghettoVCB LOG START ==============================

2013-12-04 07:57:56 -- info: CONFIG - VERSION = 2013_01_11_0
2013-12-04 07:57:56 -- info: CONFIG - GHETTOVCB_PID = $
2013-12-03 16:30:19 -- info: CONFIG - VM_BACKUP_VOLUME = /vmfs/volumes/Backup-LUN/BackupsDir
2013-12-04 07:57:56 -- info: CONFIG - VM_BACKUP_ROTATION_COUNT = 2
2013-12-04 07:57:56 -- info: CONFIG - VM_BACKUP_DIR_NAMING_CONVENTION = 2013-12-04_07-57-55
2013-12-04 07:57:56 -- info: CONFIG - DISK_BACKUP_FORMAT = thin
2013-12-04 07:57:56 -- info: CONFIG - POWER_VM_DOWN_BEFORE_BACKUP = 0
2013-12-04 07:57:56 -- info: CONFIG - ENABLE_HARD_POWER_OFF = 0
2013-12-04 07:57:56 -- info: CONFIG - ITER_TO_WAIT_SHUTDOWN = 3
2013-12-04 07:57:56 -- info: CONFIG - POWER_DOWN_TIMEOUT = 5
2013-12-04 07:57:56 -- info: CONFIG - SNAPSHOT_TIMEOUT = 15
2013-12-04 07:57:56 -- info: CONFIG - LOG_LEVEL = info
2013-12-04 07:57:56 -- info: CONFIG - BACKUP_LOG_OUTPUT = /tmp/ghettoVCB-2013-12-04_07-57-55-$.log
2013-12-04 07:57:56 -- info: CONFIG - ENABLE_COMPRESSION = 0
2013-12-04 07:57:56 -- info: CONFIG - VM_SNAPSHOT_MEMORY = 0
2013-12-04 07:57:56 -- info: CONFIG - VM_SNAPSHOT_QUIESCE = 0
2013-12-04 07:57:56 -- info: CONFIG - ALLOW_VMS_WITH_SNAPSHOTS_TO_BE_BACKEDUP = 0
2013-12-04 07:57:56 -- info: CONFIG - VMDK_FILES_TO_BACKUP = all
2013-12-04 07:57:56 -- info: CONFIG - VM_SHUTDOWN_ORDER = 
2013-12-04 07:57:56 -- info: CONFIG - VM_STARTUP_ORDER = 
2013-12-04 07:57:56 -- info: CONFIG - EMAIL_LOG = 0
2013-12-04 07:57:56 -- info: 
2013-12-04 07:57:59 -- info: Initiate backup for TestVm
Destination disk format: VMFS thin-provisioned
Cloning disk '/vmfs/volumes/VMs-LUN/TestVm/TestVm-0.vmdk'...

Clone: 9% done.
2013-12-04 07:58:03 -- info: Backup Duration: 4 Seconds
2013-12-04 07:58:03 -- info: WARN: TestVm has some Independent VMDKs that can not be backed up!

2013-12-04 07:58:05 -- info: ###### Final status: All VMs backed up OK! ######

2013-12-04 07:58:05 -- info: ============================== ghettoVCB LOG END ================================
"""

ghettovcb_output_vm_two_vmdk_poweron = """
Logging output to "/tmp/ghettoVCB-2013-12-04_08-03-34-$.log" ...
2013-12-04 08:03:34 -- info: ============================== ghettoVCB LOG START ==============================

2013-12-04 08:03:34 -- info: CONFIG - VERSION = 2013_01_11_0
2013-12-04 08:03:34 -- info: CONFIG - GHETTOVCB_PID = $
2013-12-03 16:30:19 -- info: CONFIG - VM_BACKUP_VOLUME = /vmfs/volumes/Backup-LUN/BackupsDir
2013-12-04 08:03:34 -- info: CONFIG - VM_BACKUP_ROTATION_COUNT = 2
2013-12-04 08:03:34 -- info: CONFIG - VM_BACKUP_DIR_NAMING_CONVENTION = 2013-12-04_08-03-34
2013-12-04 08:03:34 -- info: CONFIG - DISK_BACKUP_FORMAT = thin
2013-12-04 08:03:34 -- info: CONFIG - POWER_VM_DOWN_BEFORE_BACKUP = 0
2013-12-04 08:03:34 -- info: CONFIG - ENABLE_HARD_POWER_OFF = 0
2013-12-04 08:03:34 -- info: CONFIG - ITER_TO_WAIT_SHUTDOWN = 3
2013-12-04 08:03:34 -- info: CONFIG - POWER_DOWN_TIMEOUT = 5
2013-12-04 08:03:34 -- info: CONFIG - SNAPSHOT_TIMEOUT = 15
2013-12-04 08:03:34 -- info: CONFIG - LOG_LEVEL = info
2013-12-04 08:03:34 -- info: CONFIG - BACKUP_LOG_OUTPUT = /tmp/ghettoVCB-2013-12-04_08-03-34-$.log
2013-12-04 08:03:34 -- info: CONFIG - ENABLE_COMPRESSION = 0
2013-12-04 08:03:34 -- info: CONFIG - VM_SNAPSHOT_MEMORY = 0
2013-12-04 08:03:34 -- info: CONFIG - VM_SNAPSHOT_QUIESCE = 0
2013-12-04 08:03:34 -- info: CONFIG - ALLOW_VMS_WITH_SNAPSHOTS_TO_BE_BACKEDUP = 0
2013-12-04 08:03:34 -- info: CONFIG - VMDK_FILES_TO_BACKUP = all
2013-12-04 08:03:34 -- info: CONFIG - VM_SHUTDOWN_ORDER = 
2013-12-04 08:03:34 -- info: CONFIG - VM_STARTUP_ORDER = 
2013-12-04 08:03:34 -- info: CONFIG - EMAIL_LOG = 0
2013-12-04 08:03:34 -- info: 
2013-12-04 08:03:37 -- info: Initiate backup for TestVM-2
2013-12-04 08:03:37 -- info: Creating Snapshot "ghettoVCB-snapshot-2013-12-04" for TestVM-2
Destination disk format: VMFS thin-provisioned
Cloning disk '/vmfs/volumes/VMs-LUN/TestVM-2/TestVM-2-2.vmdk'...

Clone: 99% done.
Destination disk format: VMFS thin-provisioned
Cloning disk '/vmfs/volumes/SSD-LUN/TestVM-2/TestVM-2-0.vmdk'...

Clone: 100% done.
2013-12-04 08:22:45 -- info: Removing snapshot from TestVM-2 ...
2013-12-04 08:22:50 -- info: Backup Duration: 19.22 Minutes
2013-12-04 08:22:50 -- info: Successfully completed backup for TestVM-2!

2013-12-04 08:22:53 -- info: ###### Final status: All VMs backed up OK! ######

2013-12-04 08:22:53 -- info: ============================== ghettoVCB LOG END ================================
"""

class BackupProfileTests(unittest.TestCase):
    def setUp(self):
        backup.logger = Mock()
    
    def test_get_latest_backup_archives(self):
        "Check that BackupProfile.get_latest_archives behaves correctly"
        dummy_profile = {}
        with backup.BackupProfile(dummy_profile) as bp:
            bp._list_backup_archives = Mock(return_value=[
                u'C:\\Backups\\DummyVM-1-2013-01-01_01-23-45.tar.gz',
                u'C:\\Backups\\DummyVM-1-2013-02-28_21-23-45.tar.gz',
                u'C:\\Backups\\DummyVM-1-2013-03-31_11-23-45.tar.gz',
                u'/mnt/backups/DummyVM-2-2013-12-11_23-59-59.tar.gz',
            ])
            from datetime import datetime
            self.assertDictEqual(bp.get_latest_archives(), {
                u'DummyVM-1': datetime(2013,03,31,11,23,45),
                u'DummyVM-2': datetime(2013,12,11,23,59,59),
                })
    
    def test_is_vm_backup_overdue(self):
        ""
        dummy_profile = {
            u'backup_vms':  {
                u'DummyVM-1': {
                    u'period': timedelta(7),
                },
            },
        }
        with backup.BackupProfile(dummy_profile) as bp:
            from datetime import datetime
            bp._get_current_time = Mock(return_value =
                                        datetime(2013,12,11,10,9,8))
            self.assertTrue(bp.is_vm_backup_overdue(u'DummyVM-1',
                                        datetime(2013,12,1,11,11,11)))
            self.assertTrue(bp.is_vm_backup_overdue(u'DummyVM-1',
                                        datetime(2013,12,4,10,9,8)))
            self.assertFalse(bp.is_vm_backup_overdue(u'DummyVM-1',
                                        datetime(2013,12,4,10,9,9)))
            self.assertFalse(bp.is_vm_backup_overdue(u'DummyVM-1',
                                        datetime(2013,12,11,10,9,7)))
            self.assertFalse(bp.is_vm_backup_overdue(u'DummyVM-1',
                                        datetime(2013,12,12,12,12,12)))
    
    def test_get_next_vm_to_backup_no_existing_archives(self):
        "Check that next VM to backup is as expected when there are no archives"
        dummy_profile = {
            u'backup_vms':  {
                u'DummyVM-1': {
                    u'period': timedelta(7),
                },
            },
        }
        with backup.BackupProfile(dummy_profile) as bp:
            bp._list_backup_archives_for_vm = Mock(return_value=[])
            self.assertEqual(bp.get_next_vm_to_backup(), u'DummyVM-1')
            bp._list_backup_archives_for_vm.assert_called_once_with(
                u'DummyVM-1')
    
    def test_get_next_vm_to_backup_with_old_archive(self):
        "Check that next VM to backup is as expected when the last backup is old"
        dummy_profile = {
            u'backup_vms':  {
                u'DummyVM-1': {
                    u'period': timedelta(7),
                },
            },
        }
        with backup.BackupProfile(dummy_profile) as bp:
            from datetime import datetime
            bp._get_current_time = Mock(return_value =
                                        datetime(2013,12,11,10,9,8))
            bp._list_backup_archives_for_vm = Mock(return_value=[
                u'C:\\Backups\\DummyVM-1-2013-12-01_01-23-45.tar.gz',
            ])
            bp._list_backup_archives = Mock(return_value=[
                u'C:\\Backups\\DummyVM-1-2013-01-01_01-23-45.tar.gz',
                u'/mnt/backups/DummyVM-2-2013-12-11_23-59-59.tar.gz',
            ])
            self.assertEqual(bp.get_next_vm_to_backup(), u'DummyVM-1')
            bp._list_backup_archives_for_vm.assert_called_once_with(
                u'DummyVM-1')
            backup.logger.warning.assert_called_once_with(
                u'VM "DummyVM-2" not in profile, but archive found')
    
    def test_get_next_vm_to_backup_with_several_old_archives(self):
        "Check that next VM to backup is as expected when there are several old backups"
        dummy_profile = {
            u'backup_vms':  {
                u'DummyVM-1': {
                    u'period': timedelta(7),
                },
                u'DummyVM-2': {
                    u'period': timedelta(7),
                },
            },
        }
        with backup.BackupProfile(dummy_profile) as bp:
            from datetime import datetime
            bp._get_current_time = Mock(return_value =
                                        datetime(2013,12,11,10,9,8))
            bp._list_backup_archives_for_vm = Mock(return_value=[
                u'C:\\Backups\\DummyVM-1-2013-12-01_01-23-45.tar.gz',
                u'C:\\Backups\\DummyVM-2-2013-12-01_01-23-45.tar.gz',
            ])
            bp._list_backup_archives = Mock(return_value=[
                u'C:\\Backups\\DummyVM-1-2013-01-01_01-23-45.tar.gz',
                u'/mnt/backups/DummyVM-2-2012-01-01_23-59-59.tar.gz',
            ])
            self.assertEqual(bp.get_next_vm_to_backup(), u'DummyVM-2')
            bp._list_backup_archives_for_vm.assert_has_calls(
                [call(u'DummyVM-1'), call(u'DummyVM-2')])
    
    def test_get_next_vm_to_backup_with_old_and_new_archives(self):
        "Check that next VM to backup is as expected when there are old and new backups"
        dummy_profile = {
            u'backup_vms':  {
                u'DummyVM-1': {
                    u'period': timedelta(7),
                },
                u'DummyVM-2': {
                    u'period': timedelta(7),
                },
            },
        }
        with backup.BackupProfile(dummy_profile) as bp:
            from datetime import datetime
            bp._get_current_time = Mock(return_value =
                                        datetime(2013,12,11,10,9,8))
            bp._list_backup_archives_for_vm = Mock(return_value=[
                u'C:\\Backups\\DummyVM-1-2013-12-01_01-23-45.tar.gz',
                u'C:\\Backups\\DummyVM-2-2013-12-01_01-23-45.tar.gz',
            ])
            bp._list_backup_archives = Mock(return_value=[
                u'C:\\Backups\\DummyVM-1-2013-12-08_01-23-45.tar.gz',
                u'/mnt/backups/DummyVM-2-2012-01-01_23-59-59.tar.gz',
            ])
            self.assertEqual(bp.get_next_vm_to_backup(), u'DummyVM-2')
            bp._list_backup_archives_for_vm.assert_has_calls(
                [call(u'DummyVM-1'), call(u'DummyVM-2')])
    
    def test_get_next_vm_to_backup_with_new_archives(self):
        "Check that next VM to backup is as expected when there are only new backups"
        dummy_profile = {
            u'backup_vms':  {
                u'DummyVM-1': {
                    u'period': timedelta(7),
                },
                u'DummyVM-2': {
                    u'period': timedelta(7),
                },
            },
        }
        with backup.BackupProfile(dummy_profile) as bp:
            from datetime import datetime
            bp._get_current_time = Mock(return_value =
                                        datetime(2013,12,11,10,9,8))
            bp._list_backup_archives_for_vm = Mock(return_value=[
                u'C:\\Backups\\DummyVM-1-2013-12-01_01-23-45.tar.gz',
                u'C:\\Backups\\DummyVM-2-2013-12-01_01-23-45.tar.gz',
            ])
            bp._list_backup_archives = Mock(return_value=[
                u'C:\\Backups\\DummyVM-1-2013-12-08_01-23-45.tar.gz',
                u'/mnt/backups/DummyVM-2-2013-12-09_23-59-59.tar.gz',
            ])
            self.assertEqual(bp.get_next_vm_to_backup(), None)
            bp._list_backup_archives_for_vm.assert_has_calls(
                [call(u'DummyVM-1'), call(u'DummyVM-2')])
    
    def test_trim_archives_nothing_to_trim(self):
        "Check that archive trimming works as expected when there's nothing to trim"
        dummy_profile = {
            u'backup_vms':  {
                u'DummyVM-1': {
                    u'rotation_count': 3,
                },
            },
        }
        with backup.BackupProfile(dummy_profile) as bp:
            bp._list_backup_archives_for_vm = Mock(return_value=[
                u'C:\\Backups\\DummyVM-1-2013-12-01_01-23-45.tar.gz',
                u'C:\\Backups\\DummyVM-1-2013-10-01_01-23-45.tar.gz',
                u'C:\\Backups\\DummyVM-1-2013-11-01_01-23-45.tar.gz',
            ])
            bp._remove_local_file = Mock()
            bp.trim_backup_archives()
            self.assertFalse(bp._remove_local_file.called)
    
    def test_trim_archives_one_deletion(self):
        "Check that archive trimming works as expected with one archive to delete"
        dummy_profile = {
            u'backup_vms':  {
                u'DummyVM-1': {
                    u'rotation_count': 2,
                },
            },
        }
        with backup.BackupProfile(dummy_profile) as bp:
            bp._list_backup_archives_for_vm = Mock(return_value=[
                u'C:\\Backups\\DummyVM-1-2013-12-01_01-23-45.tar.gz',
                u'C:\\Backups\\DummyVM-1-2013-10-01_01-23-45.tar.gz',
                u'C:\\Backups\\DummyVM-1-2013-11-01_01-23-45.tar.gz',
            ])
            bp._remove_local_file = Mock()
            bp.trim_backup_archives()
            bp._remove_local_file.assert_called_once_with(
                u'C:\\Backups\\DummyVM-1-2013-10-01_01-23-45.tar.gz')
    
    def test_set_remote_chmod(self):
        dummy_profile = {}
        with backup.BackupProfile(dummy_profile) as bp:
            bp._run_ssh_command = Mock()
            bp._set_remote_chmod(u'script.sh')
            bp._run_ssh_command.assert_called_once_with(u'chmod +x script.sh')
    
    def test_remove_remote_file(self):
        dummy_profile = {}
        with backup.BackupProfile(dummy_profile) as bp:
            bp._run_ssh_command = Mock()
            bp._remove_remote_file(u'/file/to/remove/me.tar.gz')
            bp._run_ssh_command.assert_called_once_with(
                u'rm /file/to/remove/me.tar.gz')
    
    def test_parse_ghettovcb_output_no_vm(self):
        dummy_profile = {}
        with backup.BackupProfile(dummy_profile) as bp:
            self.assertDictContainsSubset(
                {u'FINAL_STATUS': False, u'WARNINGS': [],},
                bp._parse_ghettovcb_output(ghettovcb_output_no_vm)
            )
    
    def test_parse_ghettovcb_output_vm_with_independent_vmdk(self):
        dummy_profile = {}
        with backup.BackupProfile(dummy_profile) as bp:
            self.assertDictContainsSubset(
                {u'FINAL_STATUS': True, u'VERSION': u'2013_01_11_0',
                u'VM_BACKUP_VOLUME': u'/vmfs/volumes/Backup-LUN/BackupsDir',
                u'VM_BACKUP_ROTATION_COUNT': u'2',
                u'VM_BACKUP_DIR_NAMING_CONVENTION': u'2013-12-04_07-57-55',
                u'DISK_BACKUP_FORMAT': u'thin', u'ITER_TO_WAIT_SHUTDOWN': u'3',
                u'POWER_VM_DOWN_BEFORE_BACKUP': u'0', u'SNAPSHOT_TIMEOUT': u'15',
                u'ENABLE_HARD_POWER_OFF': '0', u'POWER_DOWN_TIMEOUT': u'5',
                u'LOG_LEVEL': u'info', u'ENABLE_COMPRESSION': u'0',
                u'BACKUP_LOG_OUTPUT': u'/tmp/ghettoVCB-2013-12-04_07-57-55-$.log',
                u'VM_SNAPSHOT_MEMORY': u'0', u'VM_SNAPSHOT_QUIESCE': u'0',
                u'ALLOW_VMS_WITH_SNAPSHOTS_TO_BE_BACKEDUP': u'0',
                u'VMDK_FILES_TO_BACKUP': u'all', u'EMAIL_LOG': u'0',
                u'BACKUP_DURATION': u'4 Seconds', u'WARNINGS': [
                u'TestVm has some Independent VMDKs that can not be backed up!'],
                },
                bp._parse_ghettovcb_output(ghettovcb_output_vm_with_independent_vmdk_poweroff)
            )
    
    def test_parse_ghettovcb_output_vm_with_two_vmdk(self):
        dummy_profile = {}
        with backup.BackupProfile(dummy_profile) as bp:
            self.assertDictContainsSubset(
                {u'FINAL_STATUS': True, u'VERSION': u'2013_01_11_0',
                u'VM_BACKUP_VOLUME': u'/vmfs/volumes/Backup-LUN/BackupsDir',
                u'VM_BACKUP_ROTATION_COUNT': u'2',
                u'VM_BACKUP_DIR_NAMING_CONVENTION': u'2013-12-04_08-03-34',
                u'DISK_BACKUP_FORMAT': u'thin', u'ITER_TO_WAIT_SHUTDOWN': u'3',
                u'POWER_VM_DOWN_BEFORE_BACKUP': u'0', u'SNAPSHOT_TIMEOUT': u'15',
                u'ENABLE_HARD_POWER_OFF': '0', u'POWER_DOWN_TIMEOUT': u'5',
                u'LOG_LEVEL': u'info', u'ENABLE_COMPRESSION': u'0',
                u'BACKUP_LOG_OUTPUT': u'/tmp/ghettoVCB-2013-12-04_08-03-34-$.log',
                u'VM_SNAPSHOT_MEMORY': u'0', u'VM_SNAPSHOT_QUIESCE': u'0',
                u'ALLOW_VMS_WITH_SNAPSHOTS_TO_BE_BACKEDUP': u'0',
                u'VMDK_FILES_TO_BACKUP': u'all', u'EMAIL_LOG': u'0',
                u'BACKUP_DURATION': u'19.22 Minutes', u'WARNINGS': [],
                },
                bp._parse_ghettovcb_output(ghettovcb_output_vm_two_vmdk_poweron)
            )
    
    def test_run_remote_backup(self):
        dummy_profile = {
            u'ghettovcb_script_template': u'/local/vmware/ghettovcb.sh.tmpl',
            u'remote_workdir':  u'/tmp',
            u'remote_backup_dir': u'/vmfs/volumes/Backup-LUN/BackupsDir',
            u'backup_vms':  {
                u'DummyVM-1': {},
            },
        }
        with backup.BackupProfile(dummy_profile) as bp:
            bp._apply_template = Mock(return_value=u'/local/tmp/rndBla')
            bp._upload_file = Mock()
            bp._set_remote_chmod = Mock()
            bp._remove_local_file = Mock()
            bp._run_ssh_command = Mock(return_value=u'ghettovcb')
            bp._remove_remote_file = Mock()
            bp._parse_ghettovcb_output = Mock(return_value={u'OK': u'OK'})
            self.assertDictEqual({u'OK': u'OK'},
                                 bp._run_remote_backup(u'DummyVM-1'))
            bp._apply_template.assert_called_once_with(
                u'/local/vmware/ghettovcb.sh.tmpl',
                {u'RemoteBackupDir': u'/vmfs/volumes/Backup-LUN/BackupsDir'}
            )
            bp._upload_file.assert_called_once_with(u'/local/tmp/rndBla',
                                                    u'/tmp/ghettovcb.sh')
            bp._set_remote_chmod.assert_called_once_with(u'/tmp/ghettovcb.sh')
            bp._remove_local_file.assert_called_once_with(u'/local/tmp/rndBla')
            bp._run_ssh_command.assert_called_once_with(
                u'/tmp/ghettovcb.sh -m DummyVM-1')
            bp._parse_ghettovcb_output.assert_called_once_with(u'ghettovcb')
    
    def test_backup_vm(self):
        dummy_profile = {
            u'ghettovcb_script_template': u'/local/vmware/ghettovcb.sh.tmpl',
            u'remote_workdir':  u'/tmp',
            u'remote_backup_dir': u'/vmfs/volumes/Backup-LUN/BackupsDir',
            u'backups_archive_dir': u'/mnt/backups/ESXi-archives',
            u'backup_vms':  {
                u'DummyVM-1': {},
            },
        }
        with backup.BackupProfile(dummy_profile) as bp:
            bp._run_remote_backup = Mock(return_value={
                u'FINAL_STATUS': True,
                u'VM_BACKUP_DIR_NAMING_CONVENTION': u'2013-12-04_08-03-34',
            })
            bp._archive_remote_backup = Mock(return_value=
                u'/vmfs/volumes/Backup-LUN/BackupsDir/DummyVM-1/DummyVM-1-2013-12-04_08-03-34.tar.gz')
            bp._download_archive = Mock(return_value=1.0)
            bp._remove_remote_file = Mock()
            bp.backup_vm(u'DummyVM-1')
            bp._run_remote_backup.assert_called_once_with(u'DummyVM-1')
            bp._archive_remote_backup.assert_called_once_with(u'DummyVM-1',
                u'DummyVM-1-2013-12-04_08-03-34')
            bp._download_archive.assert_called_once_with(
                u'/vmfs/volumes/Backup-LUN/BackupsDir/DummyVM-1/DummyVM-1-2013-12-04_08-03-34.tar.gz')
            bp._remove_remote_file.assert_called_once_with(
                u'/vmfs/volumes/Backup-LUN/BackupsDir/DummyVM-1/DummyVM-1-2013-12-04_08-03-34.tar.gz')
    
    def test_archive_remote_backup(self):
        dummy_profile = {
            u'remote_backup_dir': u'/vmfs/volumes/Backup-LUN/BackupsDir',
            u'backup_vms':  {
                u'DummyVM-1': {},
            },
        }
        with backup.BackupProfile(dummy_profile) as bp:
            bp._run_ssh_command = Mock(return_value=u'')
            self.assertEqual(u'/vmfs/volumes/Backup-LUN/BackupsDir/DummyVM-1/DummyVM-1-2013-12-04_08-03-34.tar.gz',
                bp._archive_remote_backup(u'DummyVM-1',
                u'DummyVM-1-2013-12-04_08-03-34'))
            bp._run_ssh_command.assert_called_once_with(
                u'cd "/vmfs/volumes/Backup-LUN/BackupsDir/DummyVM-1"; '
                u'tar -cz -f "DummyVM-1-2013-12-04_08-03-34.tar.gz" '
                u'"DummyVM-1-2013-12-04_08-03-34"')
    
    def test_archive_remote_backup_error(self):
        dummy_profile = {
            u'remote_backup_dir': u'/vmfs/volumes/Backup-LUN/BackupsDir',
            u'backup_vms':  {
                u'DummyVM-1': {},
            },
        }
        with backup.BackupProfile(dummy_profile) as bp:
            bp._run_ssh_command = Mock(return_value=u'No such file or directory')
            with self.assertRaises(RuntimeError) as cm:
                self.assertEqual(u'/vmfs/volumes/Backup-LUN/BackupsDir/DummyVM-1/DummyVM-1-2013-12-04_08-03-34.tar.gz',
                    bp._archive_remote_backup(u'DummyVM-1',
                    u'DummyVM-1-2013-12-04_08-03-34'))
                self.assertEqual(cm.exception.message,
                    u'Tar command failed:\nNo such file or directory')
            bp._run_ssh_command.assert_called_once_with(
                u'cd "/vmfs/volumes/Backup-LUN/BackupsDir/DummyVM-1"; '
                u'tar -cz -f "DummyVM-1-2013-12-04_08-03-34.tar.gz" '
                u'"DummyVM-1-2013-12-04_08-03-34"')
    
    def test_download_archive(self):
        dummy_profile = {
            u'host_ip': u'10.0.0.20',
            u'ftp_user': u'dummy',
            u'ftp_password': u'dummypass',
            u'backups_archive_dir': u'/mnt/backups/ESXi-archives',
            u'backup_vms':  {
                u'DummyVM-1': {},
            },
        }
        with nested(
                patch('__builtin__.open', create=True),
                patch(__name__ + '.backup.FTP', return_value=Mock()),
            ) as (mock_open, mock_ftp):
            mock_open.return_value = MagicMock(spec=file)
            with backup.BackupProfile(dummy_profile) as bp:
                self.assertIsInstance(
                    bp._download_archive(u'/vmfs/volumes/Backup-LUN/BackupsDir/DummyVM-1/DummyVM-1-2013-12-04_08-03-34.tar.gz'),
                    float)
            mock_open.assert_called_once_with(
                os.path.join(u'/mnt/backups/ESXi-archives', u'DummyVM-1-2013-12-04_08-03-34.tar.gz'),
                'wb')
            mock_file = mock_open.return_value.__enter__.return_value
            mock_ftp.assert_called_once_with(u'10.0.0.20')
            mock_ftp.return_value.login.assert_called_once_with(
                u'dummy', u'dummypass')
            mock_ftp.return_value.retrbinary.assert_called_once_with(
                u'RETR /vmfs/volumes/Backup-LUN/BackupsDir/DummyVM-1/DummyVM-1-2013-12-04_08-03-34.tar.gz',
                mock_file.write)
