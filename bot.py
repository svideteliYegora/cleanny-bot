import logging
import asyncio
import sys
import gspread
import random

import aiogram.exceptions
from aiogram.filters.callback_data import CallbackData

from cleanny_db_manager import db_manager
from resources import text

from aiogram.client.default import DefaultBotProperties
from aiogram.enums.parse_mode import ParseMode

from aiogram import Bot, Dispatcher, F, html
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import (
    InlineKeyboardBuilder,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup
)

from aiogram_calendar import SimpleCalendar, SimpleCalendarCallback

import configparser
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler


# Загрузка настроек из файла конфигураций
config = configparser.ConfigParser()
config.read('resources/config.ini')

# Получение значений из раздела Bot
tg_token = config['Bot']['token']
admin_chat_id = config['Bot']['admin_chat_id']

# Словарь для хранения данных о пользователях
users_data = {}

# Получение данных и наполнения словаря `users_data`
users = db_manager.get_all_records('Users')
if users:
    for user in users:
        tg_id = user['tg_id']
        users_data[tg_id] = {
            'user': dict(user)
        }

# Вложенный словарь с данными услуг
services_dt = db_manager.get_all_records('Services')
services_dict = {dict(i)['name']: dict(i) for i in services_dt}

# Google sheets
gc = gspread.service_account(filename='resources/true-sprite-405907-da4b97639184.json')
sh = gc.open_by_key(config['GS']['key'])

# Диспетчер, бот
dp = Dispatcher()
bot = Bot(token=tg_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))


# Планировщик
scheduler = AsyncIOScheduler()


# функции
def is_same_month(unix_timestamp: int) -> bool:
    """
    Проверяет, относится ли дата в формате Unix к текущему месяцу.

    :param unix_timestamp: Дата в формате Unix
    :return: True, если дата относится к текущему месяцу, и False в противном случае
    """
    date = datetime.fromtimestamp(unix_timestamp)

    # Получаем текущий месяц и год
    current_month = datetime.utcnow().month
    current_year = datetime.utcnow().year

    # Получаем месяц и год для заданной даты
    given_month = date.month
    given_year = date.year

    return given_month == current_month and given_year == current_year


def sum_values_of_current_week(dicty: dict) -> int:
    """
    Получение суммы отработанных часов текущей недели.

    :param dicty: Словарь, где ключи - даты, значения - отработанные часы.
    :return: Сумма отработанных часов текущей недели.
    """

    current_date = datetime.now()

    # Получение номера текущей недели
    current_week_number = current_date.isocalendar()[1]

    # Получение первого и последнего дня текущей недели
    start_of_week = current_date - timedelta(days=current_date.weekday())
    end_of_week = start_of_week + timedelta(days=6)

    total_sum = 0

    for key in dicty:
        # Если ключ представляет собой число, и оно принадлежит текущей неделе
        if key.isdigit() and start_of_week.day <= int(key) <= end_of_week.day and isinstance(dicty[key], int):
            total_sum += dicty[key]

    return total_sum


async def auto_assign_orders(**kwargs) -> None:
    """
    Функция для автоматического распределения заказа, в случае, если персонал не принимает заказ в течение часа.

    :param kwargs: Словарь с парами ключ - значение: 'staff': список сотрудников,
    'id': номер заказа,
    'msg_id': message_id,
    'chat_id': идентификатор сотрудника которому было отправлено сообщение,
    'user_id': идентификатор пользователя Telegram
    :return: None
    """

    msg_id = int(kwargs['msg_id'])
    staff_list = kwargs['staff']
    order_number = kwargs['id']
    chat_id = kwargs['chat_id']
    user_id = kwargs['user_id']

    # Удаляем сообщение о заказе так как оно не было принято
    await bot.delete_message(
        chat_id=chat_id,
        message_id=msg_id
    )

    # Распределяем заказ

    # Получаем сотрудника из списка сотрудников
    employee = random.choice(staff_list)
    fio = employee['ФИО']

    # Ставим часы сотруднику в гугл таблице
    wsh = sh.get_worksheet(0)
    row = wsh.find(fio).row
    col = datetime.fromtimestamp(users_data[user_id]['order']['appointment_datetime']).day + 1
    wsh.update_cell(row, col, users_data[user_id]['order']['total_time'] + 1)

    fio = fio.split(' ')

    # Получаем запись о сотруднике из БД
    employee_rec = db_manager.get_record('Staff', last_name=fio[0], first_name=fio[1], surname=fio[2])

    fio = " ".join(fio)
    users_data[user_id]['order_info'].update(employee_fio=fio)

    # Обновляем заказ в БД добавляя сотрудника и меняя статус
    order = db_manager.update_record('Orders', order_number, staff_id=employee_rec['id'], status='Принят')
    users_data[user_id]['order'] = order

    # Добавляем staff_id в заказ
    users_data[kwargs['user_id']]['order']['staff_id'] = employee_rec['tg_id']

    users_data[user_id]['order_info'].update(users_data[user_id]['order'])

    order_info = users_data[user_id]['order_info']
    order_info['appointment_datetime'] = datetime.fromtimestamp(order_info['appointment_datetime']).strftime("%d.%m.%Y %H:%M")
    order_info['order_date'] = datetime.fromtimestamp(order_info['order_date']).strftime("%d.%m.%Y")

    # Отправляем оповещение о заказе сотруднику, админам и пользователю
    await bot.send_message(
        chat_id=employee_rec['tg_id'],
        text=text.ORDER_MSG.format(**order_info)
    )

    await bot.send_message(
        chat_id=admin_chat_id,
        text=text.ORDER_MSG.format(**order_info)
    )

    await bot.send_message(
        chat_id=user_id,
        text=text.ORDER_MSG.format(**order_info)
    )


# kb
def create_calculate_ikb(price: int, time: float, rooms=1, bathrooms=1) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    rooms_dict = {
        1: 'Комната',
        2: 'Комнаты',
        3: 'Комнаты',
        4: 'Комнаты'
    }
    bathrooms_dict = {
        1: 'Санузел',
        2: 'Санузла',
        3: 'Санузла',
        4: 'Санузла'
    }
    room_txt = rooms_dict.get(rooms, 'Комнат')
    bathroom_txt = bathrooms_dict.get(bathrooms, 'Санузлов')

    builder.row(
        InlineKeyboardButton(text='-', callback_data=f'minus-room_{rooms}_{bathrooms}_{price}_{time}'),
        InlineKeyboardButton(text=f'{rooms} {room_txt}', callback_data='room'),
        InlineKeyboardButton(text=f'+', callback_data=f'plus-room_{rooms}_{bathrooms}_{price}_{time}'),
    )
    builder.row(
        InlineKeyboardButton(text='-', callback_data=f'minus-bathroom_{rooms}_{bathrooms}_{price}_{time}'),
        InlineKeyboardButton(text=f'{bathrooms} {bathroom_txt}', callback_data='bathroom'),
        InlineKeyboardButton(text=f'+', callback_data=f'plus-bathroom_{rooms}_{bathrooms}_{price}_{time}')
    )
    builder.row(
        InlineKeyboardButton(text='Рассчитать уборку', callback_data=f'calculate-cleaning_{price}_{time}')
    )

    return builder.as_markup()


def create_time_ikb(hours=9, minutes=0) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text='↑', callback_data=f'plus-hour_{hours}_{minutes}'),
        InlineKeyboardButton(text='↑', callback_data=f'plus-minute_{hours}_{minutes}')
    )
    builder.row(
        InlineKeyboardButton(text=f'0{hours}' if hours < 10 else str(hours), callback_data='hours'),
        InlineKeyboardButton(text=f'0{minutes}' if minutes < 10 else str(minutes), callback_data='minutes')
    )
    builder.row(
        InlineKeyboardButton(text='↓', callback_data=f'minus-hour_{hours}_{minutes}'),
        InlineKeyboardButton(text='↓', callback_data=f'minus-minute_{hours}_{minutes}')
    )
    builder.row(
        InlineKeyboardButton(text='Подтвердить', callback_data=f'confirm-time_{hours}_{minutes}')
    )

    return builder.as_markup()


def create_ikb(buttons: dict, callback_prefix=None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for key, value in buttons.items():
        builder.button(text=value, callback_data=f'{callback_prefix}_{key}' if callback_prefix else key)
    builder.adjust(1)
    return builder.as_markup()


def dict_clear(user_id: int) -> None:
    if users_data.get(user_id):
        user_dt = users_data[user_id].get('user')
        users_data[user_id].clear()
        if user_dt:
            users_data[user_id]['user'] = user_dt


# Cоздание списка с именами услуг
additional_services = [i for i in services_dt if i['additional_service']]
additional_services_buttons = ['➖' + tuple(i)[1] for i in additional_services]
additional_services_buttons.append('Подтвердить')
additional_services_buttons = dict(enumerate(additional_services_buttons))

# Клавиатура услуг
additional_services_ikb = create_ikb(additional_services_buttons, callback_prefix='additional-service')

# Клавиатура для подтверждения/изменения данных пользователя
udata_confirm_ikb = create_ikb(
    buttons={
        'edit-name': 'Изменить имя',
        'edit-last_name': 'Изменить фамилию',
        'edit-surname': 'Изменить отчество',
        'edit-address': 'Изменить адрес',
        'edit-phone': 'Изменить номер',
        'edit-email': 'Изменить Email',
        'confirm-udata': 'Подтвердить'
    },
)

# Клавиатура для выбора способа оплаты
payment_list = ['Картой по индивидуальной ссылке', 'Через интернет-банкинг', 'Наличными']
payment_ikb = create_ikb(
    buttons=dict(enumerate(payment_list)),
    callback_prefix='payment',
)

# Клавиатура для подтверждения заказа
order_checkout_ikb = create_ikb({'order-checkout': 'Оформить заказ'})

# Клавиатура для админов
admin_kb = ReplyKeyboardMarkup(
    resize_keyboard=True,
    keyboard=[
        [KeyboardButton(text='Назначить персонал'), KeyboardButton(text='Заказы')]
    ]
)


admin_orders_ikb = create_ikb(
    {
        'Активные заказы': 'active'
    }
)


@dp.message(CommandStart())
async def cmd_start_handler(msg: Message) -> None:
    user_id = msg.from_user.id
    dict_clear(user_id)
    kb = None
    staff_data = db_manager.get_staff_data(user_id)
    if staff_data['is_admin']:
        users_data[user_id]['user']['is_admin'] = True
        kb = admin_kb
    await msg.answer(text=text.WELCOME_USER_MSG.format(
        facebook=html.link('FaceBook', 'https://www.facebook.com/cleanny.happy.home/'),
        facebook_messenger=html.link('FaceBook Messanger', 'https://www.messenger.com/login.php?next=https%3A%2F%2Fwww.messenger.com%2Ft%2F1189232851104772%2F%3Fmessaging_source%3Dsource%253Apages%253Amessage_shortlink%26source_id%3D1441792%26recurring_notification%3D0'),
        instagram=html.link('Instagram', 'https://www.instagram.com/bogini_uborka/'),
        telegram=html.link('Telegram', 'https://telegram.me/cleanny_by')
    ), disable_web_page_preview=True, reply_markup=kb)


@dp.message(Command('calculate'))
async def cmd_calculate_handler(msg: Message) -> None:
    user_id = msg.from_user.id
    dict_clear(user_id)

    # Базовое количество комнат и санузлов
    order_detail = {'room': 1, 'bathroom': 1}

    users_data[user_id]['orders_services'] = {
        '+1 Комната': {
            'service_id': services_dict['+1 Комната']['id'],
            'quantity_services': 0
        },
        '+1 Санузел': {
            'service_id': services_dict['+1 Санузел']['id'],
            'quantity_services': 0
        }
    }

    # Словарь для хранения доп информации о заказе (количество ванных, количество комнат и тд)
    if users_data.get(user_id):
        users_data[user_id]['order_detail'] = order_detail
    else:
        users_data[user_id] = {
            'order_detail': order_detail
        }

    # Базовый прайс за 1 комнату + 1 санузел
    default_price = services_dict['1 Комната']['price'] + services_dict['1 Санузел']['price']

    # Базовое время выполнения работы
    default_time = float(services_dict['1 Комната']['lead_time']) + float(services_dict['1 Санузел']['lead_time'])
    await msg.answer(
        text=text.CALCULATE_MSG.format(
            total_price=html.bold(default_price),
            total_time=html.bold(default_time)
        ),
        reply_markup=create_calculate_ikb(default_price, default_time)
    )


@dp.message(Command('orders'))
async def cmd_orders_handler(msg: Message) -> None:
    user_id = msg.from_user.id
    dict_clear(user_id)

    if users_data.get(user_id, {}).get('user'):
        # Получение списка заказов пользователя
        orders_list = db_manager.cur.execute(
            """
                SELECT Orders.*, Staff.first_name, Staff.last_name, Staff.Surname
                FROM Orders
                JOIN Staff ON Orders.staff_id = Staff.id
                WHERE Orders.user_id = ? AND Orders.status IN (?, ?)
            """, (users_data[user_id]['user']['id'], 'Принят', 'Завершен')).fetchall()
        if orders_list:
            orders_list = [dict(i) for i in orders_list]

            # Частота заказов пользователя за последние 30 дней
            orders_frequency = db_manager.get_order_frequency(user_id)

            # Получение записи со скидкой в соответствие с полученной частотой заказов, если она существует
            discounts_list = db_manager.cur.execute(
                'SELECT * FROM Discounts WHERE orders_frequency <= ?',
                (orders_frequency,)
            ).fetchall()
            discount = None
            if discounts_list:
                discounts_list = [dict(i) for i in discounts_list]
                # Выбираем наибольшую скидку
                discount = discounts_list[0]
                if len(discounts_list) > 1:
                    for i in discounts_list[1:]:
                        if i['discount_value'] > discount['discount_value']:
                            discount = i

            for i in orders_list:
                dt = i
                dt['order_date'] = int(dt['order_date'])
                dt['order_date'] = datetime.fromtimestamp(dt['order_date']).strftime('%d.%m.%Y')
                dt['appointment_datetime'] = datetime.fromtimestamp(dt['appointment_datetime']).strftime('%d.%m.%Y %H:%M')
                dt['employee_fio'] = f'{dt["first_name"]} {dt["last_name"]} {dt["surname"]}'
                await msg.answer(text=text.ORDER_HISTORY_MSG.format(**dt))

            discount = discount['discount_value'] if discount else 0
            await msg.answer(text=text.DISCOUNT_MSG.format(discount=discount))
        else:
            await msg.answer(text.EMPTY_ORDER_HISTORY)
    else:
        await msg.answer(text.EMPTY_ORDER_HISTORY)


@dp.message(F.text.in_({'Назначить персонал', 'Заказы'}))
async def select_staff_handler(msg: Message) -> None:
    user_id = msg.from_user.id
    dict_clear(user_id)
    if users_data[user_id].get('user', {}).get('is_admin') or db_manager.get_staff_data(user_id)['is_admin']:
        if msg.text == 'Назначить персонал':
            users_data[user_id]['add_staff'] = True
            await msg.answer(text=text.ADD_STAFF)
        else:
            ikb = create_ikb(
                {
                 'admin-order-active': 'Активные заказы',
                 'admin-order-history': 'История заказов',
                 'admin-order-close': 'Завершить заказ',
                 }
            )
            await msg.answer(
                text=text.ACTION_MSG,
                reply_markup=ikb
            )


@dp.message(F.text)
async def input_user_data(msg: Message) -> None:
    user_id = msg.from_user.id
    u_dt = users_data[user_id]
    flag = False

    if users_data.get(user_id, {}).get('reg'):
        number = users_data[user_id]['reg']['flag']
        users_data[user_id]['reg'][number].append(msg.text)
        if number == 5:
            new_user = {i[0]: i[2] for i in users_data[user_id]['reg'].values() if isinstance(i, list)}
            new_user['tg_id'] = user_id
            rec = db_manager.insert_record('Users', **new_user)
            if rec:
                users_data[user_id]['user'] = rec
                msg_txt = text.CONFIRM_USER_DATA_MSG.format(**users_data[user_id]['user'])
                keyboard = udata_confirm_ikb
                del users_data[user_id]['reg']
                await msg.answer(
                    text=msg_txt,
                    reply_markup=keyboard
                )
                return
            else:
                await msg.answer(text.ERROR_MSG)
        users_data[user_id]['reg']['flag'] += 1
        number = users_data[user_id]['reg']['flag']
        await msg.answer(
            text=users_data[user_id]['reg'][number][1]
        )

    elif users_data.get(user_id, {}).get('add_staff'):
        tg_id = msg.from_user.id
        user_id = msg.chat.id

        del users_data[user_id]['add_staff']

        users_data[user_id]['add_staff_data'] = {'tg_id': tg_id}
        await msg.answer(text.INPUT_STAFF_FIO)

    elif users_data.get(user_id, {}).get('add_staff_data'):
        # Получаем ФИО
        last_name, first_name, surname = msg.text.split(' ')
        fio = {
            'last_name': last_name,
            'first_name': first_name,
            'surname': surname
        }
        users_data[user_id]['add_staff_data'].update(fio)
        # Спрашиваем нужно ли сделать админом
        ikb = create_ikb({'admin_yes': 'Да', 'admin_no': 'Нет'})
        await msg.answer(text=text.MAKE_ADMIN_MSG,
                         reply_markup=ikb)
    else:
        param = {}
        if u_dt.get('input_name'):
            param['input_name'] = {'first_name': msg.text}
            flag = True
        elif u_dt.get('input_last_name'):
            param['input_last_name'] = {'last_name': msg.text}
            flag = True
        elif u_dt.get('input_surname'):
            param['input_surname'] = {'surname': msg.text}
            flag = True
        elif u_dt.get('input_address'):
            param['input_address'] = {'address': msg.text}
            flag = True
        elif u_dt.get('input_phone'):
            param['input_phone'] = {'phone': msg.text}
            flag = True
        elif u_dt.get('input_email'):
            param['input_email'] = {'email': msg.text}
            flag = True

    if flag == True:
        key = tuple(param.keys())[0]
        del users_data[user_id][key]
        rec = db_manager.update_record('Users', users_data[user_id]['user']['id'], **param[key])
        if rec:
            users_data[user_id]['user'].update(rec)
            msg_txt = text.CONFIRM_USER_DATA_MSG.format(**users_data[user_id]['user'])
            keyboard = udata_confirm_ikb
            await msg.answer(
                text=msg_txt,
                reply_markup=keyboard
            )
        else:
            await msg.answer(text=text.ERROR_MSG)


@dp.callback_query(F.data.startswith('minus-room') | F.data.startswith('plus-room') | F.data.startswith('minus-bathroom') | F.data.startswith('plus-bathroom'))
async def room_and_bathroom_change_handler(cb_query: CallbackQuery) -> None:
    cb_data = cb_query.data.split('_')
    user_id = cb_query.from_user.id

    # Получение стоимости за доп 1 комнату и доп 1 санузел
    plus_room = services_dict['+1 Комната']['price']
    plus_bathroom = services_dict['+1 Санузел']['price']
    actions = {
        'minus-room': (-1, plus_room, 'room'),
        'plus-room': (1, plus_room, 'room'),
        'minus-bathroom': (-1, plus_bathroom, 'bathroom'),
        'plus-bathroom': (1, plus_bathroom, 'bathroom')
    }

    action = cb_data[0]
    change, price_change, characteristic = actions[action]
    ind = 1 if characteristic == 'room' else 2
    tm = services_dict['+1 Комната']['lead_time'] if ind < 2 else services_dict['+1 Санузел']['lead_time']
    service = '+1 Комната' if characteristic == 'room' else '+1 Санузел'
    tm = float(tm)

    # Стоимость и время уборки
    total_price = int(cb_data[3])
    total_time = float(cb_data[4])

    if action.startswith('minus'):
        if int(cb_data[ind]) > 1:
            cb_data[ind] = int(cb_data[ind]) + change
            total_price -= price_change
            total_time -= tm
            users_data[user_id]['orders_services'][service]['quantity_services'] -= 1
        else:
            return

    elif action.startswith('plus'):
        cb_data[ind] = int(cb_data[ind]) + change
        total_price += price_change
        total_time += tm
        users_data[user_id]['orders_services'][service]['quantity_services'] += 1

    users_data[user_id]['order_detail'][characteristic] += change

    await cb_query.message.edit_text(
        text=text.CALCULATE_MSG.format(
            total_price=html.bold(total_price),
            total_time=html.bold(total_time)
        ),
        reply_markup=create_calculate_ikb(total_price, total_time, int(cb_data[1]), int(cb_data[2]))
    )


@dp.callback_query(F.data.startswith('calculate-cleaning'))
async def calculate_cleaning_handler(cb_query: CallbackQuery) -> None:
    user_id = cb_query.from_user.id

    # Проверяем есть ли у пользователя активный заказ, если да то выводим сообщение
    if users_data.get(user_id, {}).get('active_order', {}).get('satus'):
        await cb_query.message.edit_text(
            text=text.PHONE_NUMBER
        )
        return

    # Получение предварительной стоимости и времени
    total_price = int(cb_query.data.split('_')[1])
    total_time = float(cb_query.data.split('_')[2])

    # Добавляем рассчитанные стоимость и время в словарь, формируя заказ
    if users_data.get(user_id):
        users_data[user_id]['order'] = {
            'total_price': total_price,
            'total_time': total_time
        }
    else:
        users_data[user_id] = {
            'order': {
                'total_price': total_price,
                'total_time': total_time
            }
        }
    calendar = SimpleCalendar()
    await cb_query.message.edit_text(
        text=text.QUESTION_ARRIVAL_TIME_MSG,
        reply_markup=await calendar.start_calendar(year=datetime.now().year, month=datetime.now().month)
    )


@dp.callback_query(SimpleCalendarCallback.filter())
async def process_simple_calendar(callback_query: CallbackQuery, callback_data: CallbackData):
    user_id = callback_query.from_user.id
    calendar = SimpleCalendar()

    # Диапазон дат от завтрашнего дня до дня через 90 дней
    start_date = datetime.now()
    end_date = start_date + timedelta(days=90)
    calendar.set_dates_range(start_date, end_date)
    selected, date = await calendar.process_selection(callback_query, callback_data)

    if selected:
        # Добавление даты в заказ
        users_data[user_id]['order']['appointment_datetime'] = date

        await callback_query.message.edit_text(
            text=text.TIME_SELECTION_PROMPT_MSG,
            reply_markup=create_time_ikb()
        )


@dp.callback_query(F.data.startswith(('plus-hour', 'plus-minute', 'minus-hour', 'minus-minute')))
async def time_choice_handler(cb_query: CallbackQuery) -> None:
    cb_data = cb_query.data.split('_')
    action, hours, minutes = cb_data[0], int(cb_data[1]), int(cb_data[2])

    if action == 'plus-hour':
        if (hours, minutes) not in ((17, 30), (18, 0)):
            hours += 1
    elif action == 'plus-minute':
        if hours != 18:
            if minutes == 30:
                minutes = 0
                hours += 1
            else:
                minutes += 30
    elif action == 'minus-hour':
        if hours != 9:
            hours -= 1
    elif action == 'minus-minute':
        if (hours, minutes) != (9, 0):
            if minutes == 0:
                minutes = 30
                hours -= 1
            else:
                minutes -= 30

    try:
        await cb_query.message.edit_text(
            text=text.TIME_SELECTION_PROMPT_MSG,
            reply_markup=create_time_ikb(hours, minutes)
        )
    except aiogram.exceptions.TelegramBadRequest as e:
        pass


@dp.callback_query(F.data.startswith('confirm'))
async def confirm_handler(cb_query: CallbackQuery) -> None:
    user_id = cb_query.from_user.id

    # Подтверждение выбранного времени
    if cb_query.data.startswith('confirm-time'):
        # Получение и добавление выбранного времени в заказ
        _, hours, minutes = cb_query.data.split('_')
        time = timedelta(hours=int(hours), minutes=int(minutes))
        users_data[user_id]['order']['appointment_datetime'] += time

        # Перезаписываем дату и время в Unix
        dt = users_data[user_id]['order']['appointment_datetime']
        users_data[user_id]['order']['appointment_datetime'] = dt.timestamp()


        # Список для хранения индексов дополнительных услуг
        users_data[user_id]['services'] = []
        await cb_query.message.edit_text(
            text=text.OPTIONS_MSG.format(
                total_price=html.bold(users_data[user_id]['order']['total_price']),
                total_time=html.bold(users_data[user_id]['order']['total_time'])
        ),
            reply_markup=additional_services_ikb
        )

    # Подтверждение данных пользователя
    elif cb_query.data.startswith('confirm-udata'):
        # Добавление адреса в заказ
        users_data[user_id]['order']['address'] = users_data[user_id]['user']['address']

        await cb_query.message.edit_text(
            text=text.PAYMENT_MSG,
            reply_markup=payment_ikb
        )

    # Подтверждение заказа персоналом
    elif cb_query.data.startswith('confirm-order-staff'):
        user_tg_id = int(cb_query.data.split('_')[1])
        order_number = int(cb_query.data.split('_')[2])
        employee_tg_id = user_id

        # Получаем запись о сотруднике из БД
        employee_rec = db_manager.get_record('Staff', tg_id=employee_tg_id)

        fio = f"{employee_rec['last_name']} {employee_rec['first_name']} {employee_rec['surname']}"
        users_data[user_tg_id]['order_info'].update(employee_fio=fio)

        # Добавляем сотрудника к заказу
        rec = db_manager.update_record('Orders', order_number, staff_id=employee_rec['id'], status='Принят')
        if rec:
            users_data[user_tg_id]['order'].update(rec)
            users_data[user_tg_id]['order_info'].update(rec)

            order_info = users_data[user_tg_id]['order_info']

            order_info['appointment_datetime'] = datetime.fromtimestamp(order_info['appointment_datetime']).strftime("%d.%m.%Y %H:%M")
            order_info['order_date'] = datetime.fromtimestamp(int(order_info['order_date'])).strftime("%d.%m.%Y")


            # Ставим часы сотруднику в гугл таблице
            wsh = sh.get_worksheet(0)
            row = wsh.find(fio).row
            col = datetime.fromtimestamp(users_data[user_tg_id]['order']['appointment_datetime']).day + 1
            wsh.update_cell(row, col, users_data[user_tg_id]['order']['total_time'] + 1)

            # Отправляем оповещение о заказе сотруднику, админам и пользователю
            await bot.edit_message_text(
                chat_id=employee_rec['tg_id'],
                text=text.ORDER_MSG.format(**order_info),
                message_id=cb_query.message.message_id
            )

            await bot.send_message(
                chat_id=admin_chat_id,
                text=text.ORDER_MSG.format(**order_info)
            )

            await bot.send_message(
                chat_id=user_tg_id,
                text=text.ORDER_MSG.format(**order_info)
            )

            # Удаляем задачу
            scheduler.remove_job(users_data[user_tg_id]['job_id'])
        else:
            await cb_query.answer(text=text.ERROR_MSG)


@dp.callback_query(F.data.startswith('additional-service'))
async def services_checkboxes_handler(cb_query: CallbackQuery) -> None:
    user_id = cb_query.from_user.id

    ind = int(cb_query.data.split('_')[1])
    if ind == len(additional_services_buttons) - 1:
        # Получение id выбранных услуг и добавление их в словарь пользователя
        services_ids = []
        for i in users_data[user_id]['services']:
            services_ids.append(additional_services[i]['id'])

        users_data[user_id]['services'] = services_ids

        # Проверка пользователя в БД
        user = users_data[user_id].get('user')
        if user:
            msg = text.CONFIRM_USER_DATA_MSG.format(**users_data[user_id]['user'])
            keyboard = udata_confirm_ikb
        else:
            msg = text.INPUT_NAME_MSG

            # Установка флага на ввод имени
            users_data[user_id]['reg'] = {
                0 : ['first_name', text.INPUT_NAME_MSG],
                1 : ['last_name', text.INPUT_LASTNAME_MSG],
                2 : ['surname', text.INPUT_SURNAME_MSG],
                3 : ['address', text.INPUT_ADDRESS_MSG],
                4 : ['phone', text.INPUT_PHONE_NUMBER_MSG],
                5 : ['email', text.INPUT_EMAIL_MSG],
                'flag': 0
            }
            keyboard = None

        await cb_query.message.edit_text(
            text=msg,
            reply_markup=keyboard
        )
        return

    btns = additional_services_buttons.copy()

    # Проверяем был ли добавлен индекс выбранной услуги, если да, то удаляем его
    if ind in users_data[user_id]['services']:
        users_data[user_id]['services'].remove(ind)
        btns[ind] = '➖' + btns[ind][1:]
        # Изменяем стоимость и время выполнения, вычитая выбранную услугу
        users_data[user_id]['order']['total_price'] -= services_dt[ind]['price']
        users_data[user_id]['order']['total_time'] -= float(services_dt[ind]['lead_time'])
    else:
        # Добавляем индексы выбранных услуг
        users_data[user_id]['services'].append(ind)
        # Изменяем стоимость и время выполнения, прибавляя выбранную услугу
        users_data[user_id]['order']['total_price'] += services_dt[ind]['price']
        users_data[user_id]['order']['total_time'] += float(services_dt[ind]['lead_time'])

    for i in users_data[user_id]['services']:
        btns[i] = '✅' + btns[i][1:]

    await cb_query.message.edit_text(
        text=text.OPTIONS_MSG.format(
            total_price=html.bold(users_data[user_id]['order']['total_price']),
            total_time=html.bold(users_data[user_id]['order']['total_time'])
        ),
        reply_markup=create_ikb(btns, callback_prefix='additional-service')
    )


@dp.callback_query(F.data.startswith('payment'))
async def payment_choice_handler(cb_query: CallbackQuery) -> None:
    user_id = cb_query.from_user.id

    # Получение способа оплаты
    ind = int(cb_query.data.split('_')[1])
    payment = payment_list[ind]

    # Добавление способа оплаты в заказ
    users_data[user_id]['order']['payment'] = payment

    # Частота заказов пользователя за последние 30 дней
    orders_frequency = db_manager.get_order_frequency(user_id)

    # Получение записи со скидкой в соответствие с полученной частотой заказов, если она существует
    discounts_list = db_manager.cur.execute(
        'SELECT * FROM Discounts WHERE orders_frequency <= ?',
        (orders_frequency, )
    ).fetchall()
    if discounts_list:
        discounts_list = [dict(i) for i in discounts_list]
        # Выбираем наибольшую скидку
        discount = discounts_list[0]
        if len(discounts_list) > 1:
            for i in discounts_list[1:]:
                if i['discount_value'] > discount['discount_value']:
                    discount = i
        users_data[user_id]['order']['discount_id'] = discount['id']

    discount = users_data[user_id]['order'].get('discount_id')
    if discount:
        discount = db_manager.get_record('Discounts', id=discount)
        discount = discount['discount_value']
    else:
        discount = 0

    # Добавляем скидку в детали к заказу
    users_data[user_id]['order_detail']['discount'] = discount

    # Сумма скидки
    discount_price = users_data[user_id]['order']['total_price'] * discount / 100

    # Обновляем общую стоимость
    users_data[user_id]['order']['total_price'] = users_data[user_id]['order']['total_price'] - discount_price

    # Получаем доп услуги выбранные пользователем
    services = []
    services_price = 0
    for service in services_dt:
        if service['id'] in users_data[user_id]['services']:
            services.append(f'{service["name"]} - {service["price"]} р')
            services_price += service['price']

    order_info = users_data[user_id]['order'].copy()
    order_info.update(users_data[user_id]['order_detail'])
    order_info['services'] = '\n'.join(services)
    order_info['price'] = round((order_info['total_price'] - services_price), 2)
    order_info['appointment_datetime'] = datetime.fromtimestamp(order_info['appointment_datetime']).strftime("%Y.%m.%d %H:%M")

    users_data[user_id]['order_info'] = order_info

    await cb_query.message.edit_text(
        text=text.ORDER_PLACEMENT_MSG.format(**order_info),
        reply_markup=order_checkout_ikb
    )


@dp.callback_query(F.data.startswith('order-checkout'))
async def order_checkout_handler(cb_query: CallbackQuery) -> None:
    user_id = cb_query.from_user.id

    # Добавляем день заказа
    today = datetime.now().timestamp()
    users_data[user_id]['order']['order_date'] = today

    # Добавляем user_id к заказу
    users_data[user_id]['order']['user_id'] = users_data[user_id]['user']['id']

    # Добавляем заказ в БД `Orders`
    rec = db_manager.insert_record('Orders', **users_data[user_id]['order'])

    if rec:
        # Перезаписываем имеющийся словарь
        users_data[user_id]['order'] = dict(rec)

        # Добавляем выбранные услуги в `OrdersServices`
        users_data[user_id]['services'].extend([services_dict['1 Комната']['id'], services_dict['1 Санузел']['id']])
        for i in users_data[user_id]['services']:
            db_manager.insert_record(
                'OrdersServices',
                order_id=users_data[user_id]['order']['id'],
                service_id=i,
                quantity_services=1
            )

        for key, value in users_data[user_id]['orders_services'].items():
            if value['quantity_services'] > 0:
                db_manager.insert_record(
                    'OrdersServices',
                    order_id=users_data[user_id]['order']['id'],
                    service_id=value['service_id'],
                    quantity_services=value['quantity_services']
                )

        # Обновляем order_info
        users_data[user_id]['order_info'].update(users_data[user_id]['order'])
        users_data[user_id]['order_info']['employee_fio'] = 'Сотрудник не выбран'

        # Сообщение о том что заказ принят и что он в обработке
        await cb_query.message.edit_text(
            text=text.ORDER_PROCESSED_MSG
        )
    else:
        await cb_query.message.edit_text(
            text=text.ERROR_MSG
        )
        return

    # День заказа
    appointment_datetime = users_data[user_id]['order']['appointment_datetime']
    order_datetime = datetime.fromtimestamp(appointment_datetime)
    day = str(order_datetime.day)

    # Время выполнения заказа + 1 час дороги
    execution_time = users_data[user_id]['order']['total_time'] + 1

    employee = None

    # Проверяем относится ли заказ к этому месяцу и есть ли свободные в этот день сотрудники
    if is_same_month(appointment_datetime):
        # Проверяем вписывается ли по времени выполнение заказа в назначенный день
        dt = order_datetime + timedelta(hours=execution_time)
        if dt.time() > datetime.strptime('21:00', '%H:%M').time():
            # Отдаем заказ на рассмотрение админу
            await bot.send_message(
                chat_id=admin_chat_id,
                text=text.ORDER_MSG.format(**users_data[user_id]['order_info'])
            )
        else:
            # Получение графика работы
            worksheet = sh.get_worksheet(0)
            worksheet = worksheet.get_all_records()

            # Список свободных и список уже работавших сотрудников у которых день заказа совпадает с рабочим днем
            free_staff = [item for item in worksheet if
                          isinstance(item[day], int) and item[day] >= 0 and sum_values_of_current_week(item) + execution_time <= 40]
            active_staff = [item for item in worksheet if
                            isinstance(item[day], int) and item[day] < 10 and sum_values_of_current_week(item) + execution_time <= 40]

            # В приоритете свободные сотрудники, но если таких нет то смотрим среди тех, кто уже работал
            if free_staff:
                employee = random.choice(free_staff)
            elif active_staff:
                # Выбираем тех сотрудников которые могут выполнить этот заказ не выйдя за рамки 10 рабочих часов в день
                buf = []
                for item in active_staff:
                    if item[day] + execution_time <= 10:
                        buf.append(item)
                active_staff = buf
                employee = random.choice(active_staff)

            if employee:
                # Получаем запись о сотруднике из БД
                fio = employee['ФИО']
                fio = fio.split(' ')

                employee_rec = db_manager.get_record('Staff', last_name=fio[0], first_name=fio[1], surname=fio[2])

                # Отправляем заказ сотруднику
                order_number = users_data[user_id]['order']['id']

                ikb = create_ikb(
                    {f'confirm-order-staff_{user_id}_{order_number}': 'Принять заказ'}
                )

                msg = await bot.send_message(
                    chat_id=employee_rec['tg_id'],
                    text=text.ORDER_PROPOSAL_MSG.format(**users_data[user_id]['order_info']),
                    reply_markup=ikb
                )

                # Закидываем задачу в планировщик и ждем час, если нет ответа от сотрудника распределяем автоматически
                tm = datetime.now() + timedelta(minutes=1)
                params = {
                    'msg_id': msg.message_id,
                    'staff': free_staff + active_staff,
                    'id': order_number,
                    'chat_id': employee_rec['tg_id'],
                    'user_id': user_id

                }
                job = scheduler.add_job(auto_assign_orders, 'date', next_run_time=tm, kwargs=params)
                users_data[user_id]['job_id'] = job.id

            else:
                # Если нет свободных сотрудников отдаем заказ админу
                await bot.send_message(
                    chat_id=admin_chat_id,
                    text=text.ORDER_MSG.format(**users_data[user_id]['order_info'])
                )


@dp.callback_query(F.data.startswith('edit'))
async def edit_handler(cb_query: CallbackQuery) -> None:
    user_id = cb_query.from_user.id

    cb_data = cb_query.data.split('-')[1]
    if cb_data == 'name':
        # Установка флага на ввод имени
        users_data[user_id]['input_name'] = True
        msg = text.INPUT_NAME_MSG

    elif cb_data == 'last_name':
        # Установка флага на ввод фамилии
        users_data[user_id]['input_last_name'] = True
        msg = text.INPUT_LASTNAME_MSG

    elif cb_data == 'surname':
        # Установка флага на ввод отчества
        users_data[user_id]['input_surname'] = True
        msg = text.INPUT_SURNAME_MSG

    elif cb_data == 'address':
        # Установка флага на ввод фамилии
        users_data[user_id]['input_address'] = True
        msg = text.INPUT_ADDRESS_MSG

    elif cb_data == 'phone':
        # Установка флага на ввод фамилии
        users_data[user_id]['input_phone'] = True
        msg = text.INPUT_PHONE_NUMBER_MSG

    elif cb_data == 'email':
        # Установка флага на ввод email
        users_data[user_id]['input_email'] = True
        msg = text.INPUT_EMAIL_MSG

    await cb_query.message.edit_text(msg)


@dp.callback_query(F.data.startswith('admin-order'))
async def admin_order_action_handler(cb_query: CallbackQuery) -> None:
   pass


@dp.callback_query(F.data.startswith('admin'))
async def add_admin_handler(cb_query: CallbackQuery) -> None:
    user_id = cb_query.from_user.id
    сb_data = cb_query.data.split('_')[1]

    if сb_data == 'yes':
        users_data[user_id]['add_staff_data']['is_admin'] = True

    # Добавляем новую запись в `Staff`
    print(users_data[user_id]['add_staff_data'])
    rec = db_manager.insert_record('Staff', **users_data[user_id]['add_staff_data'])
    if rec:
        del users_data[user_id]['add_staff_data']['is_admin']
        rec['is_admin'] = 'Да' if rec['is_admin'] else 'Нет'
        msg_txt = text.NEW_STAFF_MSG.format(**rec)
    else:
        msg_txt = text.ERROR_MSG

    await cb_query.message.edit_text(msg_txt)


async def main() -> None:
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)
        asyncio.run(main())
    finally:
        scheduler.shutdown()
