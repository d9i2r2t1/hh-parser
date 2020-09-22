import argparse
import logging
import os

from hh_parser.lib import ServiceSlave, MainProcessor

log = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument('-cfg', '--cfg_filename', nargs='+', type=str, help='manually run parser configuration(s)')
parser.add_argument('--send_email', action='store_true', help='send email with parsing results')


def main():
    args = parser.parse_args()
    slave = ServiceSlave(args)

    cfgs_files = list(set(args.cfg_filename)) \
        if args.cfg_filename else list(filter(lambda x: x.endwith('.yml'), os.listdir(slave.cfgs_path)))
    for cfg in cfgs_files:
        try:
            slave.cfg = slave.get_settings(file_name=cfg)
        except FileNotFoundError:
            log.error(f'Configuration file not found: {os.path.join(slave.cfgs_path, cfg)}')
            continue
        log.info(slave.cfg)
        try:
            worker = MainProcessor(slave.cfg, args)
            worker.run()
            worker.stop()
        except Exception as e:
            ServiceSlave(args, set_log=False).fail_notification(error=str(e), cfg_file_name=cfg)


if __name__ == '__main__':
    main()
