from backup import backup

if '__main__' == __name__:
    import argparse
    parser = argparse.ArgumentParser(description='ESXi Remote Backup Tool')
    subparsers = parser.add_subparsers()
    backup_parser = subparsers.add_parser('backup',
                                          help='Run a backup profile')
    backup_parser.add_argument('profile_name', help='Profile name to run')
    backup_parser.set_defaults(func=backup)
    args = parser.parse_args()
    try:
        args.func(**vars(args))
    except Exception, ex:
        print ex
