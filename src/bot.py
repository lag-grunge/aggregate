import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters.command import Command
from config import config
from query import Aggregator

# Включаем логирование, чтобы не пропустить важные сообщения
logging.basicConfig(level=logging.INFO)
# Объект бота
bot = Bot(token=config.bot_token.get_secret_value())
# Диспетчер
dp = Dispatcher()
# класс для получения ответов от базы
agg = Aggregator(uri='mongodb://localhost:27017', db="_aggregate", collection="pay")

# Хэндлер на команду /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(f"Hi, {message.from_user.first_name}!")

# Хэндлер на запрос к базе данных
@dp.message(F.text)
async def cmd_query(message: types.Message):
    query = message.text
    result = await agg.aggregate(query)
    await message.answer(result)

# Запуск процесса поллинга новых апдейтов
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())