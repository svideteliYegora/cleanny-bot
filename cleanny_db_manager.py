import sqlite3
from datetime import datetime, timedelta


class DBManager:
    def __init__(self, db_name):
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self.con = sqlite3.connect(db_name, check_same_thread=False)
            self.con.row_factory = sqlite3.Row
            self.cur = self.con.cursor()

    def create_db(self) -> None:
        with self.con:
            # Создание таблицы ServicesEquipment
            self.cur.execute('''CREATE TABLE IF NOT EXISTS ServicesEquipment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quantity_required INT,
                service_id INT,
                equipment_id INT,
                FOREIGN KEY (service_id) REFERENCES Services(id),
                FOREIGN KEY (equipment_id) REFERENCES Equipment(id)
            )''')

            # Создание таблицы Staff
            self.cur.execute('''CREATE TABLE IF NOT EXISTS Staff (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INT,
                first_name VARCHAR(30),
                last_name VARCHAR(30),
                surname VARCHAR(30),
                is_admin INTEGER DEFAULT 0 NOT NULL
            )''')

            # Создание таблицы Discounts
            self.cur.execute('''CREATE TABLE IF NOT EXISTS Discounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discount_value INT,
                orders_frequency INT,
                active BOOLEAN DEFAULT 1 NOT NULL
            )''')

            # Создание таблицы Orders
            self.cur.execute('''CREATE TABLE IF NOT EXISTS Orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                appointment_datetime INT,
                total_price DECIMAL(8, 2),
                total_time DECIMAL(8, 1),
                status VARCHAR(20) DEFAULT "В обработке" NOT NULL,
                address VARCHAR(100),
                payment VARCHAR(20),
                order_date INT DEFAULT (strftime('%s', 'now')) NOT NULL,
                discount_id INT,
                user_id INT,
                staff_id INT,
                FOREIGN KEY (discount_id) REFERENCES Discounts(id),
                FOREIGN KEY (user_id) REFERENCES Users(id),
                FOREIGN KEY (staff_id) REFERENCES Staff(id)
            )''')

            # Создание таблицы Users
            self.cur.execute('''CREATE TABLE IF NOT EXISTS Users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INT,
                first_name VARCHAR(30),
                last_name VARCHAR(30),
                surname VARCHAR(30),
                address VARCHAR(30),
                phone VARCHAR(20),
                email VARCHAR(100)
            )''')

            # Создание таблицы Equipment
            self.cur.execute('''CREATE TABLE IF NOT EXISTS Equipment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(50),
                type VARCHAR(50),
                quantity INT
            )''')

            # Создание таблицы Services
            self.cur.execute('''CREATE TABLE IF NOT EXISTS Services (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(50),
                description TEXT,
                lead_time DECIMAL(8, 1),
                price DECIMAL(8, 2),
                additional_service BOOLEAN DEFAULT 0 NOT NULL
            )''')

            # Создание таблицы FeedBack
            self.cur.execute('''CREATE TABLE IF NOT EXISTS FeedBack (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rating INT,
                comment TEXT,
                validate INTEGER DEFAULT 0 NOT NULL,
                order_id INT,
                FOREIGN KEY (order_id) REFERENCES Orders(id)
            )''')

            # Создание таблицы OrdersServices
            self.cur.execute('''CREATE TABLE IF NOT EXISTS OrdersServices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quantity_services INT DEFAULT 1 NOT NULL,
                order_id INT,
                service_id INT,
                FOREIGN KEY (order_id) REFERENCES Orders(id)
                FOREIGN KEY (service_id) REFERENCES Services(id)
            )''')

    def get_staff_data(self, tg_id) -> dict or None:
        """
        Метод возвращает запись из таблицы Staff по переданному параметру, если она существует, в противном случае None.

        :param tg_id: Идентификатор пользователя Telegram
        :return: Словарь или None.
        """

        query = 'SELECT * FROM Staff WHERE tg_id = ?'
        with self.con:
            data = self.con.execute(query, (tg_id, )).fetchone()
            if data:
                return dict(data)
            return None

    def get_all_records(self, table: str) -> list:
        """
        Метод возвращает все записи из указанной в параметрах таблицы.

        :param table: Название таблицы.
        :return: Словарь или None.
        """

        query = f'SELECT * FROM {table}'
        with self.con:
            return self.con.execute(query).fetchall()

    def get_records(self, table: str, **params: dict) -> list:
        """
        Получение записей из таблицы по переданным параметрам.

        :param table: Название таблицы.
        :param params: Словарь с параметрами
        :return: Список
        """

        conditions = ' AND '.join([f"{key} = ?" for key in params.keys()])
        query = f"SELECT * FROM {table} WHERE {conditions}"

        records = self.con.execute(query, tuple(params.values())).fetchall()

        return records

    def get_record(self, table: str, **params: dict) -> dict or None:
        """
        Получение одной записи (первого совпадения).

        :param table: Название таблица
        :param params: Словарь с параметрами
        :return: Словарь или None
        """

        conditions = ' AND '.join([f"{key} = ?" for key in params.keys()])
        query = f'''
            SELECT *
            FROM {table}
            WHERE {conditions}
        '''
        data = self.con.execute(query, tuple(params.values())).fetchone()
        if data:
            return dict(data)
        return data

    def get_order_frequency(self, user_id: int) -> int:
        """
        Возвращает число - частоту заказов пользователя за текущий месяц.

        :param user_id: Идентификатор пользователя в Telegram
        :return: Целое число
        """

        # Получаем текущую дату и вычитаем из нее 30 дней
        current_date = datetime.now()
        start_date = current_date - timedelta(days=30)

        current_date = int(current_date.timestamp())
        start_date = int(start_date.timestamp())

        query = '''
            SELECT Orders.* 
            FROM Orders
            JOIN Users ON Orders.user_id = Users.id
            WHERE order_date BETWEEN ? AND ?
            AND Users.tg_id = ?
        '''
        data = self.con.execute(query, (start_date, current_date, user_id)).fetchall()
        return len(data)

    def insert_record(self, table: str, **kwargs) -> dict:
        """
        Добавление новой записи в таблицу.

        :param table: Название таблицы.
        :param kwargs: словарь, содержащий пары ключ-значение, где ключи представляют собой названия полей таблицы,
        а значения - данные для вставки в эти поля.
        :return: Метод возвращает словарь с добавленной записью или пустой словарь, если запись не была добавлена.
        """

        columns = ', '.join(kwargs.keys())
        values = ', '.join(['?' for _ in kwargs.values()])
        sql_query = f"INSERT INTO {table} ({columns}) VALUES ({values})"

        try:
            with self.con:
                self.cur.execute(sql_query, tuple(kwargs.values()))
                return {'id': self.cur.lastrowid, **kwargs}
        except Exception as e:
            print(f"Ошибка при добавлении записи в таблицу {table}: {e}")
            return {}

    def update_record(self, table: str, record_id: int, **kwargs) -> dict:
        """
        Обновление записи в таблице.

        :param table: Название таблицы.
        :param record_id: Идентификатор записи, которую необходимо обновить.
        :param kwargs: Словарь, где пары ключ - значение: поле - новое значение.
        :return: Обновленную запись в виде словаря или пустой словарь, если запись не была обновлена.
        """
        if not kwargs:
            print("Нет данных для обновления")
            return {}

        set_clause = ', '.join([f"{column} = ?" for column in kwargs.keys()])
        sql_query = f"UPDATE {table} SET {set_clause} WHERE id = ?"

        try:
            with self.con:
                cursor = self.con.cursor()
                values = tuple(kwargs.values()) + (record_id,)
                cursor.execute(sql_query, values)
                rows_updated = cursor.rowcount
                if rows_updated > 0:
                    cursor.execute(f"SELECT * FROM {table} WHERE id = ?", (record_id,))
                    updated_record = cursor.fetchone()
                    if updated_record:
                        updated_record_dict = dict(updated_record)
                        print("Запись успешно обновлена")
                        return updated_record_dict
                print("Запись не была обновлена")
                return {}
        except Exception as e:
            print(f"Ошибка при обновлении записи в таблице {table}: {e}")
            return {}


db_manager = DBManager("cleanny_db.db")


def main():
    db_manager.create_db()


if __name__ == '__main__':
    main()
