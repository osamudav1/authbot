"""MongoDB replica-set storage. Every ledger mutation commits atomically."""
import time
import re
from pymongo import MongoClient, ReturnDocument
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from .domain import MIN_PVP_WAGER, PVP_REQUEST_TIMEOUT_SECONDS, RuleError, money


class MongoStore:
    def __init__(self, uri, database):
        self.client = MongoClient(uri, serverSelectionTimeoutMS=10000, connectTimeoutMS=10000,
                                  socketTimeoutMS=20000, appname="authbid-bot")
        self.db = self.client[database]
        hello = self.client.admin.command("hello")
        if not hello.get("setName") and hello.get("msg") != "isdbgrid":
            self.close()
            raise ValueError("MongoDB replica set or Atlas is required for wallet transactions")
        self.db.bids.create_index([("chat_id", 1), ("message_id", 1)], unique=True)
        self.db.bids.create_index([("user_id", 1), ("auction_id", 1), ("id", -1)])
        self.db.auctions.create_index([("status", 1), ("ends", 1)])
        self.db.auctions.update_many(
            {"winner_notified": {"$exists": False}, "status": {"$in": ["closed", "cancelled"]}},
            {"$set": {"winner_notified": 1}})
        self.db.auctions.update_many({"winner_notified": {"$exists": False}},
                                     {"$set": {"winner_notified": 0}})
        self.db.holds.create_index("user_id")
        self.db.wallet_events.create_index("event_key", unique=True)
        self.db.wallet_events.create_index([("user_id", 1), ("id", -1)])
        self.db.messages.create_index([("chat_id", 1), ("message_id", 1)], unique=True)
        self.db.pvp_games.create_index([("group_id", 1), ("status", 1), ("next_at", 1)])
        self.db.pvp_games.create_index([("status", 1), ("requester_id", 1), ("target_id", 1)])
        self.db.coord.update_one({"_id":"ledger"}, {"$setOnInsert":{"version":0}}, upsert=True)
        # Upgrade only the untouched legacy default; preserve an owner's custom setting.
        self.db.settings.update_one({"_id":"increment","value":"5000"}, {"$set":{"value":"25000"}})
        for key, value in (("wallet_mode","1"),("increment","25000"),("paused","0"),
                           ("rules","Bid ငွေကို ယာယီထိန်းထားပြီး winner ကို လေလံပိတ်ချိန် ငွေဖြတ်ပါမယ်။")):
            self.db.settings.update_one({"_id":key},{"$setOnInsert":{"value":value}},upsert=True)

    def close(self):
        self.client.close()

    def _tx(self, operation):
        def execute(session):
            # A shared write serializes ledger decisions, including cross-auction holds.
            # Driver retries write conflicts by rerunning the entire callback.
            self.db.coord.update_one({"_id":"ledger"},{"$inc":{"version":1}},session=session)
            return operation(session)
        with self.client.start_session() as session:
            return session.with_transaction(execute, read_concern=ReadConcern("snapshot"),
                                            write_concern=WriteConcern("majority"), max_commit_time_ms=10000)

    def _next(self, name, session):
        return self.db.counters.find_one_and_update({"_id":name},{"$inc":{"value":1}},upsert=True,
                   return_document=ReturnDocument.AFTER,session=session)["value"]

    @staticmethod
    def _clean(row):
        return {k:v for k,v in row.items() if k!="_id"} if row else None

    def get(self, key, default="", session=None):
        row=self.db.settings.find_one({"_id":key},session=session)
        return row["value"] if row else default

    def set(self, key, value):
        if key=="wallet_mode" and str(value)!="1":
            raise RuleError("Wallet hold ကို ပိတ်၍မရပါ။ Bid အတွက် balance လိုအပ်ပါတယ်။")
        self._tx(lambda s:self.db.settings.update_one({"_id":key},{"$set":{"value":str(value)}},upsert=True,session=s))

    def claim_new_user(self, user_id):
        result = self.db.new_users.update_one(
            {"_id": user_id}, {"$setOnInsert": {"created": int(time.time())}}, upsert=True)
        return result.upserted_id is not None

    def target(self, key, value):
        def change(s):
            if key=="group_id" and str(value)==self.get("pvp_group_id",session=s):
                raise RuleError("PvP group နဲ့ auction discussion group ကို သီးခြားထားပါ။")
            if self.get(key,session=s)==str(value): return
            if self.db.auctions.find_one({"status":{"$in":["active","publishing"]}},session=s):
                raise RuleError("Channel/group ပြောင်းမယ်ဆို active/publishing လေလံတွေကို အရင်ပိတ်ပါ။")
            self.db.settings.update_one({"_id":key},{"$set":{"value":str(value)}},upsert=True,session=s)
        self._tx(change)

    def set_pvp_group(self, group_id):
        def configure(s):
            if str(group_id)==str(self.get("group_id",session=s)):
                raise RuleError("PvP group နဲ့ auction discussion group ကို သီးခြားထားပါ။")
            if str(group_id) != str(self.get("pvp_group_id", session=s)) and self.db.pvp_games.find_one(
                    {"status":{"$in":["pending","running"]}}, session=s):
                raise RuleError("PvP group ပြောင်းရန် pending/running ပွဲများကို အရင်ရှင်းပါ။")
            self.db.settings.update_one({"_id":"pvp_group_id"},{"$set":{"value":str(group_id)}},upsert=True,session=s)
        self._tx(configure)

    def auction(self, auction_id, session=None):
        row=self.db.auctions.find_one({"_id":auction_id},session=session)
        if not row: raise RuleError("လေလံ ID မတွေ့ပါ။")
        return self._clean(row)

    def create(self, card, now=None):
        def create(s):
            created=int(time.time()) if now is None else now
            duration=card.get("duration_seconds")
            if duration is not None and (type(duration) is not int or not 1<=duration<=604800):
                raise RuleError("ကြာချိန်ကို 1sec မှ 7days အတွင်း သတ်မှတ်ပါ။")
            ends=created+duration if duration is not None else card["ends"]
            if ends<=created: raise RuleError("End time က အနာဂတ်ဖြစ်ရပါမယ်။")
            channel,group=self.get("channel_id",session=s),self.get("group_id",session=s)
            if not channel or not group: raise RuleError("/setchannel နှင့် /setgroup အရင်သတ်မှတ်ပါ။")
            aid=self._next("auctions",s)
            row={k:card[k] for k in ("photo","name","anime","rarity","start")}
            row["media_type"] = card.get("media_type", "photo")
            row.update(_id=aid,id=aid,increment=int(self.get("increment",session=s)),ends=ends,
                       channel_id=int(channel),group_id=int(group),post_id=None,root_id=None,
                       status="publishing",highest=None,winner_id=None,winner_name=None,
                       winner_notified=0,version=1,rendered=0,created=created,duration_seconds=duration,wallet_required=1)
            self.db.auctions.insert_one(row,session=s)
            return aid
        return self._tx(create)

    def _published(self, aid, post_id, published_at, s):
        row=self.auction(aid,s)
        if row["status"]!="publishing": return
        ends=published_at+row["duration_seconds"] if row.get("duration_seconds") else row["ends"]
        self.db.auctions.update_one({"_id":aid},{"$set":{"post_id":post_id,"status":"active","ends":ends},"$inc":{"version":1}},session=s)

    def published(self, auction_id, post_id, published_at=None):
        at=int(time.time()) if published_at is None else published_at
        self._tx(lambda s:self._published(auction_id,post_id,at,s))

    def attach(self, auction_id, post_id, root_id, published_at=None):
        def attach(s):
            row=self.auction(auction_id,s)
            if row["post_id"] not in (None,post_id) or row["root_id"] not in (None,root_id): return False
            self.db.auctions.update_one({"_id":auction_id},{"$set":{"post_id":post_id,"root_id":root_id}},session=s)
            self._published(auction_id,post_id,int(time.time()) if published_at is None else published_at,s)
            self._map(row["group_id"],root_id,auction_id,s)
            return True
        return self._tx(attach)

    def _map(self, chat_id, message_id, auction_id, session=None):
        self.db.messages.update_one({"_id":f"{chat_id}:{message_id}"},
            {"$setOnInsert":{"chat_id":chat_id,"message_id":message_id,"auction_id":auction_id}},upsert=True,session=session)

    def map_message(self, chat_id, message_id, auction_id):
        self._map(chat_id,message_id,auction_id)

    def thread_auction(self, chat_id, root_id):
        row=self.db.auctions.find_one({"group_id":chat_id,"root_id":root_id},{"_id":1})
        return row["_id"] if row else None

    def resolve(self, chat_id, message_ids):
        matches=set()
        for mid in message_ids:
            if mid:
                row=self.db.messages.find_one({"_id":f"{chat_id}:{mid}"})
                if row:matches.add(row["auction_id"])
        return matches.pop() if len(matches)==1 else None

    def search_active(self, query="", offset=0, limit=20, now=None):
        if not 0<=offset<=1000000 or not 1<=limit<=50 or len(query)>256:
            raise RuleError("Search parameter မမှန်ပါ။")
        at=int(time.time()) if now is None else now
        filters=dict(status="active",ends={"$gt":at},post_id={"$ne":None},
                     channel_id=int(self.get("channel_id") or 0),group_id=int(self.get("group_id") or 0))
        query=query.strip()
        if query:
            literal={"$regex":re.escape(query),"$options":"i"}
            filters["$or"]=[{field:literal} for field in ("name","anime","rarity")]
            number=query.lstrip('#')
            if number.isascii() and number.isdigit() and len(number)<19:
                filters["$or"].append({"_id":int(number)})
        rows=[self._clean(r) for r in self.db.auctions.find(filters).sort("id",-1).skip(offset).limit(limit+1)]
        return rows[:limit],len(rows)>limit

    def find_post(self, channel_id, post_id, group_id):
        return self._clean(self.db.auctions.find_one(dict(channel_id=channel_id,post_id=post_id,group_id=group_id)))

    def recoverable(self, auction_id, channel_id, group_id):
        return self._clean(self.db.auctions.find_one(dict(_id=auction_id,channel_id=channel_id,group_id=group_id,status="publishing",post_id=None)))

    def wallet_balance(self, user_id, session=None):
        if session is None:
            return self._tx(lambda s:self.wallet_balance(user_id,s))
        row=self.db.wallets.find_one({"_id":user_id},session=session)
        total=row["balance"] if row else 0
        held=sum(r["amount"] for r in self.db.holds.find({"user_id":user_id},session=session))
        return {"total":total,"held":held,"available":total-held}

    def bid(self, auction_id, user_id, user_name, amount, chat_id, message_id, now=None):
        if type(amount) is not int or not 0<amount<=99999999999: raise RuleError("Bid ပမာဏ မမှန်ပါ။")
        def bid(s):
            at=int(time.time()) if now is None else now
            row=self.auction(auction_id,s)
            if row["group_id"]!=chat_id or str(chat_id)!=self.get("group_id",session=s):
                raise RuleError("သတ်မှတ်ထားတဲ့ group မှာပဲ bid ဆွဲနိုင်ပါတယ်။")
            if self.db.bids.find_one(dict(chat_id=chat_id,message_id=message_id),session=s):return False
            if self.get("paused",session=s)=="1":raise RuleError("Owner က bidding ခဏရပ်ထားပါတယ်။")
            if self.db.banned.find_one({"_id":user_id},session=s):raise RuleError("သင့်အကောင့်ကို bid ဆွဲခွင့်ပိတ်ထားပါတယ်။")
            if row["status"]!="active" or at>=row["ends"]:raise RuleError("ဒီလေလံ ပိတ်သွားပါပြီ။")
            minimum=row["start"] if row["highest"] is None else row["highest"]+row["increment"]
            if amount<minimum:raise RuleError(f"အနည်းဆုံး {money(minimum)} bid ဆွဲရပါမယ်။")
            balance=self.wallet_balance(user_id,s)
            hold=self.db.holds.find_one({"_id":auction_id,"user_id":user_id},session=s)
            usable=balance["available"]+(hold["amount"] if hold else 0)
            if amount>usable:raise RuleError(f"လက်ကျန်မလုံလောက်ပါ။ ဒီ bid အတွက် သုံးနိုင်ငွေ {money(usable)}။ /bal ကိုစစ်ပါ။")
            self.db.holds.replace_one({"_id":auction_id},dict(_id=auction_id,auction_id=auction_id,user_id=user_id,amount=amount),upsert=True,session=s)
            bid_id=self._next("bids",s)
            self.db.bids.insert_one(dict(_id=bid_id,id=bid_id,auction_id=auction_id,user_id=user_id,user_name=user_name,amount=amount,created=at,chat_id=chat_id,message_id=message_id),session=s)
            self.db.auctions.update_one({"_id":auction_id},{"$set":{"highest":amount,"winner_id":user_id,"winner_name":user_name,"wallet_required":1},"$inc":{"version":1}},session=s)
            return True
        return self._tx(bid)

    def is_banned(self,user_id):return bool(self.db.banned.find_one({"_id":user_id}))

    def ban(self,user_id,blocked):
        def change(s):
            if blocked:self.db.banned.update_one({"_id":user_id},{"$set":{"user_id":user_id}},upsert=True,session=s)
            else:self.db.banned.delete_one({"_id":user_id},session=s)
        self._tx(change)

    def banned(self):return [r["user_id"] for r in self.db.banned.find().sort("user_id",1).limit(100)]

    def _finish(self, aid, status, s):
        if status not in ("closed","cancelled"):raise RuleError("လေလံ status မမှန်ပါ။")
        row=self.auction(aid,s)
        allowed=("active","publishing") if status=="cancelled" else ("active",)
        if row["status"] not in allowed:raise RuleError("ဒီလေလံကို ယခုလုပ်ဆောင်ချက် မလုပ်နိုင်ပါ။")
        if status=="closed" and row["wallet_required"] and row["highest"] is not None:
            hold=self.db.holds.find_one({"_id":aid},session=s)
            if not hold or hold["user_id"]!=row["winner_id"] or hold["amount"]!=row["highest"]:
                raise RuleError("Wallet hold မကိုက်ညီပါ။ Owner က စစ်ဆေးရန်လိုပါတယ်။")
            result=self.db.wallets.update_one({"_id":hold["user_id"],"balance":{"$gte":hold["amount"]}}, {"$inc":{"balance":-hold["amount"]}},session=s)
            if result.modified_count!=1:raise RuleError("Winner balance မလုံလောက်ပါ။")
            eid=self._next("wallet_events",s)
            self.db.wallet_events.insert_one(dict(_id=eid,id=eid,user_id=hold["user_id"],delta=-hold["amount"],kind="win",note="Auction purchase",actor_id=None,auction_id=aid,event_key=f"settle:{aid}",created=int(time.time())),session=s)
        self.db.holds.delete_one({"_id":aid},session=s)
        self.db.auctions.update_one({"_id":aid},{"$set":{"status":status},"$inc":{"version":1}},session=s)

    def finish(self,auction_id,status):self._tx(lambda s:self._finish(auction_id,status,s))

    def close_due(self,now=None):
        at=int(time.time()) if now is None else now
        # Avoid starting write transactions on every idle UI tick.
        for row in self.db.auctions.find({"status":"active","ends":{"$lte":at}}, {"_id":1}):
            def close(s,aid=row["_id"]):
                current=self.auction(aid,s)
                if current["status"]=="active" and current["ends"]<=at:self._finish(aid,"closed",s)
            self._tx(close)

    def extend(self,auction_id,minutes):
        if type(minutes) is not int or not 1<=minutes<=10080:raise RuleError("1–10080 minutes သာရေးပါ။")
        def extend(s):
            row=self.auction(auction_id,s)
            if row["status"]!="active" or row["ends"]<=time.time():raise RuleError("Active လေလံကိုပဲ အချိန်တိုးနိုင်ပါတယ်။")
            self.db.auctions.update_one({"_id":auction_id},{"$inc":{"ends":minutes*60,"version":1}},session=s)
        self._tx(extend)

    def pending_winners(self):
        return list(self.db.auctions.find({"status": "closed", "winner_id": {"$ne": None},
                    "winner_notified": 0, "root_id": {"$ne": None}, "post_id": {"$ne": None}}).sort("id", 1))

    def winner_notified(self, auction_id):
        self.db.auctions.update_one({"_id": auction_id}, {"$set": {"winner_notified": 1}})

    def dirty(self):return [self._clean(r) for r in self.db.auctions.find({"post_id":{"$ne":None},"$expr":{"$gt":["$version","$rendered"]}}).sort("id",1)]
    def rendered(self,auction_id,version):self.db.auctions.update_one({"_id":auction_id},{"$max":{"rendered":version}})
    def listing(self,limit=30):return [self._clean(r) for r in self.db.auctions.find().sort("id",-1).limit(limit)]
    def history(self,auction_id,limit=30):
        self.auction(auction_id)
        return [self._clean(r) for r in self.db.bids.find({"auction_id":auction_id}).sort("id",-1).limit(limit)]
    def export_bids(self,auction_id):return (self._clean(r) for r in self.db.bids.find({"auction_id":auction_id}).sort("id",1))

    def user_history(self,user_id,wins=False,now=None):
        at=int(time.time()) if now is None else now
        pipeline=[{"$match":{"user_id":user_id}},{"$group":{"_id":"$auction_id","my_bid":{"$max":"$amount"},"last_bid":{"$max":"$id"}}},{"$sort":{"last_bid":-1}},
                  {"$lookup":{"from":"auctions","localField":"_id","foreignField":"_id","as":"auction"}},{"$unwind":"$auction"}]
        if wins:pipeline.append({"$match":{"auction.winner_id":user_id,"$or":[{"auction.status":"closed"},{"auction.status":"active","auction.ends":{"$lte":at}}]}})
        pipeline.append({"$limit":10})
        return [dict(self._clean(r["auction"]),my_bid=r["my_bid"],last_bid=r["last_bid"]) for r in self.db.bids.aggregate(pipeline)]

    def active_auctions(self,page=0,now=None):
        at=int(time.time()) if now is None else now
        query=dict(status="active",ends={"$gt":at},channel_id=int(self.get("channel_id") or 0),group_id=int(self.get("group_id") or 0))
        count=self.db.auctions.count_documents(query)
        page=min(page,max(0,(count-1)//5))
        rows=self.db.auctions.find(query).sort([("ends",1),("id",1)]).skip(page*5).limit(5)
        return count,[self._clean(r) for r in rows],page

    def adjust_wallet(self,user_id,delta,actor_id,event_key,note=""):
        if type(user_id) is not int or not 0<user_id<2**63 or type(delta) is not int or not 0<abs(delta)<=99999999999 or len(note)>200:
            raise RuleError("User ID၊ ပမာဏ သို့မဟုတ် note (200 စာလုံးအထိ) မမှန်ပါ။")
        def adjust(s):
            existing=self.db.wallet_events.find_one({"event_key":event_key},session=s)
            if existing:
                if (existing["user_id"],existing["delta"],existing["actor_id"])!=(user_id,delta,actor_id):raise RuleError("ဒီ request ကို ပြင်ပြီးပြန်သုံးလို့မရပါ။")
                return False
            balance=self.wallet_balance(user_id,s)
            if balance["available"]+delta<0:raise RuleError("ထိန်းထားတဲ့ bid ငွေကို နုတ်လို့မရပါ။ Available balance မလုံလောက်ပါ။")
            if balance["total"]+delta>99999999999:raise RuleError("Wallet ပမာဏအများဆုံး ကျော်နေပါတယ်။")
            self.db.wallets.update_one({"_id":user_id},{"$inc":{"balance":delta},"$set":{"user_id":user_id}},upsert=True,session=s)
            eid=self._next("wallet_events",s)
            self.db.wallet_events.insert_one(dict(_id=eid,id=eid,user_id=user_id,delta=delta,kind="credit" if delta>0 else "debit",note=note,actor_id=actor_id,auction_id=None,event_key=event_key,created=int(time.time())),session=s)
            return True
        return self._tx(adjust)

    def wallet_history(self,user_id):return [self._clean(r) for r in self.db.wallet_events.find({"user_id":user_id}).sort("id",-1).limit(10)]

    def transfer_coins(self,group_id,sender_id,recipient_id,amount,event_key):
        if (type(sender_id) is not int or type(recipient_id) is not int
                or not 0<sender_id<2**63 or not 0<recipient_id<2**63
                or sender_id==recipient_id or type(amount) is not int
                or not 0<amount<=99999999999 or not event_key):
            raise RuleError("Coin gift ပမာဏ သို့မဟုတ် user ID မမှန်ပါ။")
        sent_key,received_key=f"{event_key}:sent",f"{event_key}:received"
        def transfer(s):
            if str(group_id)!=str(self.get("pvp_group_id",session=s)):
                raise RuleError("Coin gift ကို သတ်မှတ်ထားတဲ့ PvP game group မှာပဲ ပို့နိုင်ပါတယ်။")
            rows=list(self.db.wallet_events.find({"event_key":{"$in":[sent_key,received_key]}},session=s))
            if rows:
                existing={r["event_key"]:(r["user_id"],r["delta"],r["actor_id"]) for r in rows}
                expected={sent_key:(sender_id,-amount,sender_id),received_key:(recipient_id,amount,sender_id)}
                if existing==expected:return False
                raise RuleError("ဒီ gift request ကို ပြင်ပြီးပြန်သုံးလို့မရပါ။")
            sender=self.wallet_balance(sender_id,s)
            if sender["available"]<amount:raise RuleError(f"Coin မလုံလောက်ပါ။ လက်ရှိသုံးနိုင်တာ {money(sender['available'])} ပါ။")
            recipient=self.wallet_balance(recipient_id,s)
            if recipient["total"]+amount>99999999999:raise RuleError("လက်ခံသူ၏ wallet ပမာဏအများဆုံး ကျော်နေပါတယ်။")
            self.db.wallets.update_one({"_id":sender_id},{"$inc":{"balance":-amount},"$set":{"user_id":sender_id}},upsert=True,session=s)
            self.db.wallets.update_one({"_id":recipient_id},{"$inc":{"balance":amount},"$set":{"user_id":recipient_id}},upsert=True,session=s)
            created=int(time.time())
            for uid,delta,kind,note,key in (
                (sender_id,-amount,"gift_sent",f"Gift to user {recipient_id}",sent_key),
                (recipient_id,amount,"gift_received",f"Gift from user {sender_id}",received_key)):
                eid=self._next("wallet_events",s)
                self.db.wallet_events.insert_one(dict(_id=eid,id=eid,user_id=uid,delta=delta,kind=kind,
                    note=note,actor_id=sender_id,auction_id=None,event_key=key,created=created),session=s)
            return True
        return self._tx(transfer)

    def _pvp_game(self, game_id, session=None):
        row=self.db.pvp_games.find_one({"_id":game_id},session=session)
        if not row:raise RuleError("PvP request မတွေ့ပါ။")
        return self._clean(row)

    def create_pvp(self,game_id,group_id,requester_id,requester_name,target_id,target_name,amount,now=None):
        if type(amount) is not int or not MIN_PVP_WAGER<=amount<=99999999999:raise RuleError("PvP အနည်းဆုံးလောင်းကြေး 250 coin ဖြစ်ရပါမယ်။")
        if requester_id==target_id:raise RuleError("ကိုယ့်ကိုယ်ကို PvP request လုပ်လို့မရပါ။")
        def create(s):
            at=time.time() if now is None else now
            if str(group_id)!=str(self.get("pvp_group_id",session=s)):
                raise RuleError("သတ်မှတ်ထားတဲ့ PvP group မှာပဲ ကစားနိုင်ပါတယ်။")
            if self.db.pvp_games.count_documents({"group_id":group_id,"status":"running"},session=s)>=5:
                raise RuleError("လက်ရှိ ပွဲ ၅ ပွဲ ကစားနေပါတယ်။ တစ်ပွဲပြီးမှ ပွဲအသစ်တောင်းနိုင်ပါတယ်။")
            for uid in (requester_id,target_id):
                if self.db.pvp_games.find_one({"group_id":group_id,"status":"running","$or":[{"requester_id":uid},{"target_id":uid}]},session=s):
                    raise RuleError("This User Playing")
            if self.db.pvp_games.find_one({"status":"pending","requester_id":requester_id},session=s):
                raise RuleError("သင့်မှာ အဖြေမရသေးတဲ့ PvP request ရှိပါတယ်။")
            balance=self.wallet_balance(requester_id,s)
            if balance["available"]<amount:raise RuleError(f"Coin မလုံလောက်ပါ။ လက်ရှိသုံးနိုင်တာ {money(balance['available'])} ပါ။")
            self.db.pvp_games.insert_one(dict(_id=game_id,id=game_id,group_id=group_id,requester_id=requester_id,
                requester_name=requester_name[:64],target_id=target_id,target_name=target_name[:64],amount=amount,
                status="pending",message_id=0,created=at,next_at=None,step=0,final_percent=None,winner_id=None,
                slot_notified=0),session=s)
            return self._pvp_game(game_id,s)
        return self._tx(create)

    def set_pvp_message(self,game_id,message_id):
        self.db.pvp_games.update_one({"_id":game_id,"status":"pending"},{"$set":{"message_id":message_id}})

    def cancel_pvp(self,game_id,actor_id):
        def cancel(s):
            row=self._pvp_game(game_id,s)
            if actor_id not in (row["requester_id"],row["target_id"]):raise RuleError("ဒီ PvP request ကို cancel လုပ်ခွင့်မရှိပါ။")
            if row["status"]!="pending":raise RuleError("ဒီ PvP request ကို ယခု cancel မလုပ်နိုင်ပါ။")
            self.db.pvp_games.update_one({"_id":game_id,"status":"pending"},{"$set":{"status":"cancelled"}},session=s)
            return self._pvp_game(game_id,s)
        return self._tx(cancel)

    def expire_pvp(self, now=None):
        at = time.time() if now is None else now
        cutoff = at - PVP_REQUEST_TIMEOUT_SECONDS
        def expire(s):
            rows = list(self.db.pvp_games.find(
                {"status": "pending", "created": {"$lte": cutoff}}, session=s))
            expired = []
            for row in rows:
                result = self.db.pvp_games.update_one(
                    {"_id": row["_id"], "status": "pending"},
                    {"$set": {"status": "cancelled"}}, session=s)
                if result.modified_count:
                    row["status"] = "cancelled"
                    expired.append(self._clean(row))
            return expired
        return self._tx(expire)

    def accept_pvp(self,game_id,actor_id,final_percent,now=None):
        if type(final_percent) is not int or not 1<=final_percent<=100:raise RuleError("PvP result မမှန်ပါ။")
        def accept(s):
            at=time.time() if now is None else now
            row=self._pvp_game(game_id,s)
            if actor_id!=row["target_id"]:raise RuleError("Request လက်ခံနိုင်သူက ဖိတ်ခေါ်ခံရသူတစ်ဦးတည်းပါ။")
            if row["status"]!="pending":raise RuleError("ဒီ PvP request ကို အရင်ဖြေပြီးပါပြီ။")
            if str(row["group_id"])!=str(self.get("pvp_group_id",session=s)):raise RuleError("ဒီ group မှာ PvP မကစားနိုင်တော့ပါ။")
            active_rounds = self.db.pvp_games.count_documents({"group_id":row["group_id"],"status":"running"},session=s)
            if active_rounds>=5:
                raise RuleError("လက်ရှိ ပွဲ ၅ ပွဲ ကစားနေပါတယ်။ တစ်ပွဲပြီးမှ ထပ်စနိုင်ပါတယ်။")
            for uid in (row["requester_id"],row["target_id"]):
                if self.db.pvp_games.find_one({"group_id":row["group_id"],"status":"running","$or":[{"requester_id":uid},{"target_id":uid}]},session=s):
                    raise RuleError("This User Playing")
                balance=self.wallet_balance(uid,s)
                if balance["available"]<row["amount"]:raise RuleError(f"User {uid} မှာ လိုအပ်တဲ့ coin မလုံလောက်ပါ။")
                if balance["total"]+row["amount"]>99999999999:raise RuleError("လောင်းကြေးအနိုင်ရလျှင် wallet limit ကျော်နိုင်ပါတယ်။")
            for uid in (row["requester_id"],row["target_id"]):
                self.db.wallets.update_one({"_id":uid,"balance":{"$gte":row["amount"]}},{"$inc":{"balance":-row["amount"]}},session=s)
                eid=self._next("wallet_events",s)
                self.db.wallet_events.insert_one(dict(_id=eid,id=eid,user_id=uid,delta=-row["amount"],kind="pvp_stake",
                    note=f"PvP stake · {game_id}",actor_id=actor_id,auction_id=None,event_key=f"pvp:{game_id}:stake:{uid}",created=at),session=s)
            self.db.pvp_games.update_one({"_id":game_id,"status":"pending"},{"$set":{"status":"running","next_at":at+1,"step":0,"final_percent":final_percent,
                # Only the fifth concurrent round can free a slot from a full set of five.
                "slot_notified": 0 if active_rounds == 4 else 1}},session=s)
            return self._pvp_game(game_id,s)
        return self._tx(accept)

    def due_pvp(self,now=None):
        at=time.time() if now is None else now
        return [self._clean(r) for r in self.db.pvp_games.find({"status":"running","next_at":{"$lte":at}}).sort([("next_at",1),("created",1)])]

    def advance_pvp(self,game_id,now=None):
        def advance(s):
            at=time.time() if now is None else now
            row=self._pvp_game(game_id,s)
            if row["status"]!="running" or row["next_at"] is None or row["next_at"]>at:return row
            step=row["step"]+1
            if step<5:
                self.db.pvp_games.update_one({"_id":game_id,"status":"running"},{"$set":{"step":step,"next_at":at+1}},session=s)
                return self._pvp_game(game_id,s)
            requester_percent = row["final_percent"]
            target_percent = 100 - requester_percent
            winner = row["requester_id"] if requester_percent > 50 else row["target_id"]
            loser = row["target_id"] if winner == row["requester_id"] else row["requester_id"]
            loser_percent = target_percent if winner == row["requester_id"] else requester_percent
            pot = row["amount"] * 2
            loser_payout = 0 if loser_percent > 25 else pot * loser_percent // 100
            winner_payout = pot - loser_payout
            for uid in (winner, loser):
                if not self.db.wallets.find_one({"_id":uid},session=s):
                    raise RuleError("Winner/loser wallet မတွေ့ပါ။ Owner က စစ်ဆေးရန်လိုပါတယ်။")
            self.db.wallets.update_one({"_id":winner},{"$inc":{"balance":winner_payout}},session=s)
            eid=self._next("wallet_events",s)
            self.db.wallet_events.insert_one(dict(_id=eid,id=eid,user_id=winner,delta=winner_payout,kind="pvp_win",
                note=f"PvP prize · {game_id}",actor_id=None,auction_id=None,event_key=f"pvp:{game_id}:prize",created=at),session=s)
            if loser_payout:
                self.db.wallets.update_one({"_id":loser},{"$inc":{"balance":loser_payout}},session=s)
                eid=self._next("wallet_events",s)
                self.db.wallet_events.insert_one(dict(_id=eid,id=eid,user_id=loser,delta=loser_payout,kind="pvp_refund",
                    note=f"PvP refund · {game_id}",actor_id=None,auction_id=None,event_key=f"pvp:{game_id}:refund",created=at),session=s)
            self.db.pvp_games.update_one({"_id":game_id,"status":"running"},{"$set":{"status":"finished","winner_id":winner,"step":5,"next_at":None}},session=s)
            return self._pvp_game(game_id,s)
        return self._tx(advance)

    def pending_pvp_slot_notifications(self):
        return [self._clean(r) for r in self.db.pvp_games.find({"status":"finished","slot_notified":0}).sort([("created",1),("_id",1)])]

    def mark_pvp_slot_notified(self,game_id):
        self.db.pvp_games.update_one({"_id":game_id,"status":"finished"},{"$set":{"slot_notified":1}})

    def stats(self):
        counts=[(r["_id"],r["count"]) for r in self.db.auctions.aggregate([{"$group":{"_id":"$status","count":{"$sum":1}}}])]
        sales=list(self.db.auctions.aggregate([{"$match":{"status":"closed"}},{"$group":{"_id":None,"total":{"$sum":"$highest"}}}]))
        return counts,self.db.bids.count_documents({}),sales[0]["total"] if sales else 0
