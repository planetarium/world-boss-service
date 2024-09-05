import asyncio
import base64
import datetime
import hashlib
import typing

import bencodex
import boto3
import ethereum_kms_signer  # type: ignore
from ethereum_kms_signer.spki import SPKIRecord  # type: ignore
from gql import Client
from gql.dsl import DSLMutation, DSLQuery, DSLSchema, dsl_gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.requests import RequestsHTTPTransport
from pyasn1.codec.der.decoder import decode as der_decode  # type: ignore
from pyasn1.codec.der.encoder import encode as der_encode  # type: ignore
from pyasn1.type.univ import Integer, SequenceOf  # type: ignore
from sqlalchemy.orm import Session

from world_boss.app.config import config
from world_boss.app.enums import NetworkType
from world_boss.app.models import Transaction
from world_boss.app.raid import get_jwt_auth_header, get_transfer_assets_plain_value
from world_boss.app.stubs import AmountDictionary, CurrencyDictionary, Recipient
from world_boss.app.transaction import create_unsigned_tx


class KmsWorldBossSigner:
    def __init__(self, key_id: str):
        self._key_id = key_id
        self._cached_public_key = None
        self._cached_address = None

    @property
    def public_key(self) -> bytes:
        if self._cached_public_key is None:
            client = boto3.client("kms")
            public_key_der = client.get_public_key(KeyId=self._key_id)["PublicKey"]
            received_record, _ = der_decode(public_key_der, asn1Spec=SPKIRecord())
            self._cached_public_key = received_record["subjectPublicKey"].asOctets()
        return self._cached_public_key

    @property
    def address(self) -> str:
        if self._cached_address is None:
            self._cached_address = ethereum_kms_signer.get_eth_address(self._key_id)
        return self._cached_address

    def _get_client(self, headless_url: str) -> Client:
        transport = RequestsHTTPTransport(
            url=headless_url, headers=get_jwt_auth_header()
        )
        return Client(transport=transport, fetch_schema_from_transport=True)

    def _get_async_client(self, headless_url: str) -> Client:
        transport = AIOHTTPTransport(url=headless_url, headers=get_jwt_auth_header())
        return Client(transport=transport, fetch_schema_from_transport=True)

    def _sign_and_save(
        self, unsigned_transaction: bytes, nonce: int, db: Session
    ) -> Transaction:
        account = ethereum_kms_signer.kms.BasicKmsAccount(self._key_id, self.address)
        msg_hash = hashlib.sha256(unsigned_transaction).digest()
        _, r, s = account.sign_msg_hash(msg_hash).vrs

        n = int.from_bytes(
            base64.b64decode("/////////////////////rqu3OavSKA7v9JejNA2QUE="), "big"
        )

        seq = SequenceOf(componentType=Integer())
        seq.extend([r, min(s, n - s)])
        signature = der_encode(seq)
        signed_transaction = self._sign_transaction(unsigned_transaction, signature)
        return self._save_transaction(signed_transaction, nonce, db)

    def _sign_transaction(self, unsigned_transaction: bytes, signature: bytes) -> bytes:
        decoded = bencodex.loads(unsigned_transaction)
        decoded[b"S"] = signature
        return bencodex.dumps(decoded)

    def _save_transaction(
        self, signed_transaction: bytes, nonce, db: Session
    ) -> Transaction:
        transaction = Transaction()
        tx_id = hashlib.sha256(signed_transaction).hexdigest()
        transaction.tx_id = tx_id
        transaction.nonce = nonce
        transaction.signer = self.address
        transaction.payload = signed_transaction.hex()
        db.add(transaction)
        db.commit()
        return transaction

    def transfer_assets(
        self,
        time_stamp: datetime.datetime,
        nonce: int,
        recipients: typing.List[Recipient],
        memo: str,
        db: Session,
    ) -> Transaction:
        pv = get_transfer_assets_plain_value(self.address, recipients, memo)
        unsigned_transaction = create_unsigned_tx(
            "0x000000000000", self.public_key, self.address, nonce, pv, time_stamp
        )
        return self._sign_and_save(unsigned_transaction, nonce, db)

    def prepare_reward_assets(
        self, headless_url: str, assets: typing.List[AmountDictionary]
    ) -> str:
        client = self._get_client(headless_url)
        with client as session:
            assert client.schema is not None
            ds = DSLSchema(client.schema)
            query = dsl_gql(
                DSLQuery(
                    ds.StandaloneQuery.actionQuery().select(
                        ds.ActionQuery.prepareRewardAssets.args(
                            rewardPoolAddress=self.address, assets=assets
                        )
                    )
                )
            )
            result = session.execute(query)
            return result["actionQuery"]["prepareRewardAssets"]

    def stage_transaction(self, headless_url: str, transaction: Transaction) -> str:
        client = self._get_client(headless_url)
        with client as session:
            assert client.schema is not None
            ds = DSLSchema(client.schema)
            query = dsl_gql(
                DSLMutation(
                    ds.StandaloneMutation.stageTransaction.args(
                        payload=transaction.payload
                    )
                )
            )
            result = session.execute(query)
            return result["stageTransaction"]

    def query_transaction_result(
        self, headless_url: str, tx_id: str, db: Session
    ) -> str:
        client = self._get_client(headless_url)
        with client as session:
            assert client.schema is not None
            ds = DSLSchema(client.schema)
            query = dsl_gql(
                DSLQuery(
                    ds.StandaloneQuery.transaction.select(
                        ds.TransactionHeadlessQuery.transactionResult.args(
                            txId=tx_id
                        ).select(
                            ds.TxResultType.txStatus,
                        )
                    )
                )
            )
            result = session.execute(query)
            tx_result = result["transaction"]["transactionResult"]
            tx_status = tx_result["txStatus"]
            transaction = db.query(Transaction).filter_by(tx_id=tx_id).one()
            transaction.tx_result = tx_status
            db.add(transaction)
            db.commit()
            return tx_status

    def query_balance(self, headless_url: str, currency: CurrencyDictionary) -> str:
        client = self._get_client(headless_url)
        with client as session:
            assert client.schema is not None
            ds = DSLSchema(client.schema)
            query = dsl_gql(
                DSLQuery(
                    ds.StandaloneQuery.stateQuery.select(
                        ds.StateQuery.balance.args(
                            address=self.address,
                            currency=currency,
                        ).select(
                            ds.FungibleAssetValueWithCurrencyType.quantity,
                            ds.FungibleAssetValueWithCurrencyType.currency.select(
                                ds.CurrencyType.ticker
                            ),
                        )
                    )
                )
            )
            result = session.execute(query)
            balance = result["stateQuery"]["balance"]["quantity"]
            ticker = result["stateQuery"]["balance"]["currency"]["ticker"]
            return f"{balance} {ticker}"

    async def stage_transactions_async(self, network_type: NetworkType, db: Session):
        headless_urls = [config.headless_url]
        transactions = (
            db.query(Transaction).filter_by(tx_result=None).order_by(Transaction.nonce)
        )
        result = await asyncio.gather(
            *[
                self.stage_transaction_async(headless_url, transaction)
                for headless_url in headless_urls
                for transaction in transactions
            ]
        )
        return result

    async def stage_transaction_async(
        self, headless_url: str, transaction: Transaction
    ) -> str:
        client = self._get_async_client(headless_url)
        async with client as session:
            assert client.schema is not None
            ds = DSLSchema(client.schema)
            query = dsl_gql(
                DSLMutation(
                    ds.StandaloneMutation.stageTransaction.args(
                        payload=transaction.payload
                    )
                )
            )
            result = await session.execute(query)
            return result["stageTransaction"]

    async def check_transaction_status_async(
        self, network_type: NetworkType, db: Session
    ):
        headless_url = config.headless_url
        transactions = (
            db.query(Transaction).filter_by(tx_result=None).order_by(Transaction.nonce)
        )
        await asyncio.gather(
            *[
                self.query_transaction_result_async(headless_url, transaction, db)
                for transaction in transactions
            ]
        )
        db.commit()

    async def query_transaction_result_async(
        self, headless_url: str, transaction: Transaction, db: Session
    ):
        client = self._get_async_client(headless_url)
        async with client as session:
            assert client.schema is not None
            ds = DSLSchema(client.schema)
            query = dsl_gql(
                DSLQuery(
                    ds.StandaloneQuery.transaction.select(
                        ds.TransactionHeadlessQuery.transactionResult.args(
                            txId=transaction.tx_id
                        ).select(
                            ds.TxResultType.txStatus,
                        )
                    )
                )
            )
            result = await session.execute(query)
            tx_result = result["transaction"]["transactionResult"]
            tx_status = tx_result["txStatus"]
            transaction.tx_result = tx_status
            db.add(transaction)


signer = KmsWorldBossSigner(config.kms_key_id)
