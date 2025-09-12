import asyncio
from aiosmtpd.controller import Controller

class PrintHandler:
    async def handle_DATA(self, server, session, envelope):
        print("=== Nowa wiadomość ===")
        print(f"Od: {envelope.mail_from}")
        print(f"Do: {envelope.rcpt_tos}")
        print("Treść:")
        print(envelope.content.decode("utf8", errors="replace"))
        print("=======================")
        return "250 OK"

if __name__ == "__main__":
    handler = PrintHandler()
    controller = Controller(handler, hostname="127.0.0.1", port=1025)
    controller.start()

    print("SMTP serwer działa na 127.0.0.1:1025")
    try:
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        pass
