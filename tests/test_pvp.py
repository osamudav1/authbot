import unittest
import tempfile
import asyncio
from pathlib import Path
from types import SimpleNamespace

from auction_bot.store import RuleError, Store, cents, money
from auction_bot.bot import AuctionBot, auth_adjustment, pvp_animation_text


class PvPStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(str(Path(self.temp.name) / "bot.sqlite3"))
        self.store.set_pvp_group(-100123)
        self.amount = cents("500")

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def credit(self, user_id, amount=100000):
        self.store.adjust_wallet(user_id, amount, 99, f"credit:{user_id}", "test coin")

    def request(self, game_id, first, second, now=100):
        return self.store.create_pvp(game_id, -100123, first, f"Player {first}",
                                     second, f"Player {second}", self.amount, now=now)

    def test_request_needs_funds_and_only_target_can_confirm(self):
        self.credit(1, 25000)
        with self.assertRaisesRegex(RuleError, "Coin မလုံလောက်"):
            self.request("short", 1, 2)
        self.store.adjust_wallet(1, 75000, 99, "credit:1:extra", "top up")
        self.credit(2, 25000)
        game = self.request("challenge", 1, 2)
        with self.assertRaisesRegex(RuleError, "ဖိတ်ခေါ်ခံရသူ"):
            self.store.accept_pvp(game["id"], 1, 60, now=100)
        with self.assertRaisesRegex(RuleError, "လိုအပ်တဲ့ coin"):
            self.store.accept_pvp(game["id"], 2, 60, now=100)
        self.store.adjust_wallet(2, 75000, 99, "credit:2:extra", "top up")
        accepted = self.store.accept_pvp(game["id"], 2, 60, now=100)
        self.assertEqual(accepted["status"], "running")
        self.assertEqual(self.store.wallet_balance(1)["available"], 50000)
        self.assertEqual(self.store.wallet_balance(2)["available"], 50000)
        with self.assertRaisesRegex(RuleError, "This User Playing"):
            self.request("busy", 3, 1)

    def test_winner_receives_pot_and_slot_notice_is_emitted_once(self):
        self.credit(1)
        self.credit(2)
        game = self.request("payout", 1, 2)
        self.store.accept_pvp(game["id"], 2, 60, now=100)
        self.assertEqual(self.store.due_pvp(now=100), [])
        for tick in range(1, 6):
            game = self.store.advance_pvp(game["id"], now=100 + tick)
            self.assertEqual(game["step"], tick)
        self.assertEqual(game["status"], "finished")
        self.assertEqual(game["winner_id"], 1)
        self.assertEqual(self.store.wallet_balance(1)["total"], 150000)
        self.assertEqual(self.store.wallet_balance(2)["total"], 50000)
        notifications = self.store.pending_pvp_slot_notifications()
        self.assertEqual([row["id"] for row in notifications], ["payout"])
        self.store.mark_pvp_slot_notified("payout")
        self.assertEqual(self.store.pending_pvp_slot_notifications(), [])

    def test_cannot_accept_a_sixth_simultaneous_round(self):
        for user_id in range(1, 13):
            self.credit(user_id, 50000)
        for index in range(5):
            game = self.request(f"game-{index}", index * 2 + 1, index * 2 + 2)
            self.store.accept_pvp(game["id"], game["target_id"], 60, now=100)
        with self.assertRaisesRegex(RuleError, "ပွဲ ၅ ပွဲ"):
            self.request("game-5", 11, 12)

    def test_coin_amount_display(self):
        self.assertEqual(cents("10000"), 1_000_000)
        self.assertEqual(money(1_000_000), "10,000.00 coin")

    def test_pvp_group_must_remain_separate_from_auction_group(self):
        with self.assertRaisesRegex(RuleError, "သီးခြားထားပါ"):
            self.store.target("group_id", -100123)

    def test_final_animation_shows_odds_and_winner(self):
        text = pvp_animation_text({
            "status": "finished", "final_percent": 60, "step": 5,
            "requester_id": 1, "requester_name": "Alice",
            "target_id": 2, "target_name": "Bob", "winner_id": 1,
            "amount": cents("500"),
        })
        self.assertIn("60%", text)
        self.assertIn("40%", text)
        self.assertIn("Winner:", text)
        self.assertIn("Prize: 1,000.00 coin", text)

    def test_owner_can_credit_or_debit_by_reply_and_by_id(self):
        reply = SimpleNamespace(
            sender_chat=None,
            from_user=SimpleNamespace(id=77, is_bot=False),
        )
        message = SimpleNamespace(reply_to_message=reply)
        owner_bot = object.__new__(AuctionBot)
        self.assertEqual(auth_adjustment(["+", "500", "gift"], message),
                         (77, cents("500"), "gift"))
        self.assertEqual(auth_adjustment(["-5"], message),
                         (77, -cents("5"), ""))
        self.assertEqual(owner_bot.owner_wallet_adjustment("credit", ["77", "500", "gift"], message),
                         (77, cents("500"), "gift"))
        self.assertEqual(owner_bot.owner_wallet_adjustment("debit", ["-5"], message),
                         (77, -cents("5"), ""))

    def test_button_cooldown_is_two_seconds(self):
        owner_bot = object.__new__(AuctionBot)
        owner_bot.config = SimpleNamespace(owners=set())
        owner_bot.button_cooldown_until = {}
        user = SimpleNamespace(id=77, is_bot=False)
        self.assertEqual(owner_bot.button_cooldown(user), 0.0)
        self.assertAlmostEqual(owner_bot.button_cooldown(user), 2.0, delta=0.01)
        owner_bot.button_cooldown_until = {}
        owner_bot.config.owners = {99}
        owner = SimpleNamespace(id=99, is_bot=False)
        self.assertEqual(owner_bot.button_cooldown(owner), 0.0)
        self.assertAlmostEqual(owner_bot.button_cooldown(owner), 2.0, delta=0.01)

    def test_credit_notification_is_sent_to_users_bot_dm(self):
        class FakeBot:
            def __init__(self):
                self.sent = []

            async def send_message(self, **kwargs):
                self.sent.append(kwargs)

        owner_bot = object.__new__(AuctionBot)
        fake = FakeBot()
        delivered = asyncio.run(owner_bot.wallet_credit_notification(
            fake, 77, cents("500"), cents("600")))
        self.assertTrue(delivered)
        self.assertEqual(fake.sent[0]["chat_id"], 77)
        self.assertIn("500.00 coin", fake.sent[0]["text"])
        self.assertIn("600.00 coin", fake.sent[0]["text"])

    def test_pvp_wager_below_500_coins_is_rejected(self):
        self.credit(1)
        self.credit(2)
        with self.assertRaisesRegex(RuleError, "အနည်းဆုံးလောင်းကြေး 500 coin"):
            self.store.create_pvp("too-small", -100123, 1, "Player 1", 2, "Player 2", cents("499"))


if __name__ == "__main__":
    unittest.main()
