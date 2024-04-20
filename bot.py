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
    'order_number': номер заказа, 'msg_id': message_id,
    'chat_id': идентификатор сотрудника которому было отправлено сообщение,
    'user_id': идентификатор пользователя Telegram
    :return: None
    """

    msg_id = int(kwargs['msg_id'])
    staff_list = kwargs['staff']
    order_number = kwargs['order_number']
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
    fio = fio.split(' ')

    # Получаем запись о сотруднике из БД
    employee_rec = db_manager.get_record('Staff', last_name=fio[0], first_name=fio[1], surname=fio[2])

    fio = " ".join(fio)
    users_data[user_id]['order_info'].update(employee_fio=fio)

    # Обновляем заказ в БД добавляя сотрудника
    order = db_manager.update_record('Orders', order_number, staff_id=employee_rec['id'])
    users_data[user_id]['order'] = order

    # Добавляем staff_id в заказ
    users_data[kwargs['user_id']]['order']['staff_id'] = employee_rec['tg_id']
    await bot.send_message(
        chat_id=employee_rec['tg_id'],
        text=text.ORDER_MSG.format()
    )

    # Отправляем оповещение о заказе сотруднику, админам и пользователю
    await bot.send_message(
        chat_id=employee_rec['tg_id'],
        text=text.ORDER_MSG.format(**users_data[user_id]['order_info'])
    )

    await bot.send_message(
        chat_id=admin_chat_id,
        text=text.ORDER_MSG.format(**users_data[user_id]['order_info'])
    )

    await bot.send_message(
        chat_id=user_id,
        text=text.ORDER_MSG.format(**users_data[user_id]['order_info'])
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


# @dp.message(F.text)
# async def msg_handler(msg: Message) -> None:
#     await msg.answer(text=str(msg.from_user.id))


@dp.message(CommandStart())
async def cmd_start_handler(msg: Message) -> None:
    user_id = msg.from_user.id

    # Проверка наличия пользователя в таблице Staff
    staff_data = db_manager.get_staff_data(user_id)
    if staff_data:
        # Является ли пользователь админом
        if staff_data['is_admin']:
            await msg.answer(text='Пользователь является админом')
        else:
            await msg.answer(text='Пользователь является обслуживающим персоналом')
    else:
        await msg.answer(text=text.WELCOME_USER_MSG.format(
            facebook=html.link('FaceBook', 'https://www.facebook.com/cleanny.happy.home/'),
            facebook_messenger=html.link('FaceBook Messanger', 'https://www.messenger.com/login.php?next=https%3A%2F%2Fwww.messenger.com%2Ft%2F1189232851104772%2F%3Fmessaging_source%3Dsource%253Apages%253Amessage_shortlink%26source_id%3D1441792%26recurring_notification%3D0'),
            instagram=html.link('Instagram', 'https://www.instagram.com/bogini_uborka/'),
            telegram=html.link('Telegram', 'https://telegram.me/cleanny_by')
        ), disable_web_page_preview=True)


@dp.message(Command('calculate'))
async def cmd_calculate_handler(msg: Message) -> None:
    user_id = msg.from_user.id

    # Базовое количество комнат и санузлов
    order_detail = {'room': 1, 'bathroom': 1}

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
    tm = float(tm)

    # Стоимость и время уборки
    total_price = int(cb_data[3])
    total_time = float(cb_data[4])

    if action.startswith('minus'):
        if int(cb_data[ind]) > 1:
            cb_data[ind] = int(cb_data[ind]) + change
            total_price -= price_change
            total_time -= tm
        else:
            return

    elif action.startswith('plus'):
        cb_data[ind] = int(cb_data[ind]) + change
        total_price += price_change
        total_time += tm

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
            msg = text.INPUT_ADDRESS_MSG
            # Установка флага на ввод адреса
            users_data[user_id]['input_user_data'] = True
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
    discounts_list = db_manager.get_records('Discounts', orders_frequency=orders_frequency, active=True)
    if discounts_list:
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
        users_data[user_id]['order'] = rec

        # Добавляем в order_info номер заказа
        users_data[user_id]['order_info']['order_number'] = users_data[user_id]['order']['id']

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
    order_datetime = datetime.utcfromtimestamp(appointment_datetime)
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
                employee_tg_id = employee_rec['tg_id']
                order_number = users_data[user_id]['order']['id']
                ikb = create_ikb(
                    {f'confirm-order-staff_{employee_tg_id}_{order_number}': 'Принять заказ'}
                )
                msg = await bot.send_message(
                    chat_id=employee_rec['tg_id'],
                    text=text.ORDER_PROPOSAL_MSG.format(**users_data[user_id]['order_info']),
                    reply_markup=ikb
                )

                # Закидываем задачу в планировщик и ждем час, если нет ответа от сотрудника распределяем автоматически
                tm = datetime.now() + timedelta(hours=1)
                params = {
                    'msg_id': msg.message_id,
                    'staff': free_staff + active_staff,
                    'order_number': order_number,
                    'chat_id': employee_rec['tg_id'],
                    'user_id': user_id

                }
                scheduler.add_job(auto_assign_orders, 'date', next_run_time=tm, kwargs=params)

            else:
                # Если нет свободных сотрудников отдаем заказ админу
                await bot.send_message(
                    chat_id=admin_chat_id,
                    text=text.ORDER_MSG.format(**users_data[user_id]['order_info'])
                )



@dp.callback_query(F.data.startswith('edit'))
async def edit_handler(cb_query: CallbackQuery) -> None:
    pass


async def main() -> None:
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)
        asyncio.run(main())
    finally:
        scheduler.shutdown()
