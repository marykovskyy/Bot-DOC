from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

TOKEN = '8239908904:AAETuzhmsxYO7Bxu7Lic4mFut0d8HQ9LQpk'  # Замініть на реальний

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('Привіт! Бот працює.')

def main():
    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.run_polling()

if __name__ == '__main__':
    main()