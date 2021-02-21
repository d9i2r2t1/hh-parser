import argparse
import logging
import os

from .lib.main_processor import MainProcessor
from .lib.service_funcs import exception_notify, remove_old_files, set_logging, read_yml_config

log = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument('-cfg', '--cfg_filename', nargs='+', type=str, help='manually run parser configuration(s)')
parser.add_argument('--send_email', action='store_true', help='send email with parsing results')

CONFIGS_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), 'configs'))
LOGS_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), 'logs'))


def main():
    logs_file_path = set_logging(logs_folder=LOGS_FOLDER)
    args = parser.parse_args()

    config_files = list(set(args.cfg_filename)) if args.cfg_filename \
        else list(filter(lambda x: x.endswith('.yml') and x != 'cfg_example.yml', os.listdir(CONFIGS_FOLDER)))
    for config_file in config_files:
        try:
            config_path = os.path.join(CONFIGS_FOLDER, config_file)
            config = read_yml_config(file_path=config_path)
            service_email_params = {
                'smtp_host': config.service_mail.server,
                'smtp_port': config.service_mail.port,
                'smtp_login': config.service_mail.login,
                'smtp_password': config.service_mail.password,
                'smtp_use_ssl': config.service_mail.ssl,
                'email_to': config.service_mail.email_to,
                'email_from': config.service_mail.email_from,
                'log_file_path': logs_file_path,
            }
        except FileNotFoundError:
            log.warning(f'Configuration file not found: {config_file}')
            continue

        @exception_notify(**service_email_params)
        def run(cfg, arguments):
            worker = MainProcessor(cfg, arguments)
            worker.run()
            worker.stop()

        run(config, args)

    remove_old_files(folder_path=LOGS_FOLDER, lifetime_days=180, file_type='.log')


if __name__ == '__main__':
    main()
