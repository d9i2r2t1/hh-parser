import sys
from lib.service_slave import ServiceSlave
from lib.data_fetcher import HhParser
from lib.data_etl import DataEtl
from lib.email_sender import EmailSender


def main(flags):
    # Настраиваем логирование, получаем настройки из файла settings
    slave = ServiceSlave(flags)
    settings = slave.get_settings()

    # Получаем, обрабатываем и сохраняем данные
    etl = DataEtl(HhParser(settings).parse(), settings)
    etl.save_report_file()
    etl.set_etl_data_to_bd()


if __name__ == "__main__":
    try:
        main(sys.argv)
    except Exception as e:
        ServiceSlave(sys.argv, set_log=False).fail_notification(str(e))
