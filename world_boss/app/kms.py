import asyncio
import base64
import datetime
import hashlib
import typing

import boto3
import ethereum_kms_signer  # type: ignore
from ethereum_kms_signer.spki import SPKIRecord  # type: ignore
from gql import Client
from gql.dsl import DSLMutation, DSLQuery, DSLSchema, dsl_gql
from gql.transport.httpx import HTTPXAsyncTransport, HTTPXTransport
from pyasn1.codec.der.decoder import decode as der_decode  # type: ignore
from pyasn1.codec.der.encoder import encode as der_encode  # type: ignore
from pyasn1.type.univ import Integer, SequenceOf  # type: ignore

from world_boss.app.config import config
from world_boss.app.enums import NetworkType
from world_boss.app.models import Transaction
from world_boss.app.orm import db
from world_boss.app.stubs import AmountDictionary, CurrencyDictionary, Recipient

MINER_URLS: dict[NetworkType, str] = {
    NetworkType.MAIN: "http://9c-main-validator-1.nine-chronicles.com/graphql",
    NetworkType.INTERNAL: "http://9c-internal-miner-1.nine-chronicles.com/graphql",
}

HEADLESS_URLS: dict[NetworkType, typing.List[str]] = {
    NetworkType.MAIN: [
        MINER_URLS[NetworkType.MAIN],
    ],
    NetworkType.INTERNAL: [
        MINER_URLS[NetworkType.INTERNAL],
        "http://9c-internal-rpc-1.nine-chronicles.com/graphql",
    ],
}


class KmsWorldBossSigner:
    def __init__(self, key_id: str):
        self._key_id = key_id

    @property
    def public_key(self) -> bytes:
        client = boto3.client("kms")
        public_key_der = client.get_public_key(KeyId=self._key_id)["PublicKey"]
        received_record, _ = der_decode(public_key_der, asn1Spec=SPKIRecord())

        return received_record["subjectPublicKey"].asOctets()

    @property
    def address(self) -> str:
        return ethereum_kms_signer.get_eth_address(self._key_id)

    def _get_client(self, headless_url: str) -> Client:
        transport = HTTPXTransport(url=headless_url)
        return Client(transport=transport, fetch_schema_from_transport=True)

    def _get_async_client(self, headless_url: str) -> Client:
        transport = HTTPXAsyncTransport(url=headless_url)
        return Client(transport=transport, fetch_schema_from_transport=True)

    def _sign_and_save(
        self, headless_url: str, unsigned_transaction: bytes, nonce: int
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
        signed_transaction = self._sign_transaction(
            headless_url, unsigned_transaction, signature
        )
        return self._save_transaction(signed_transaction, nonce)

    def _sign_transaction(
        self, headless_url: str, unsigned_transaction: bytes, signature: bytes
    ) -> bytes:
        client = self._get_client(headless_url)
        with client as session:
            assert client.schema is not None
            ds = DSLSchema(client.schema)
            query = dsl_gql(
                DSLQuery(
                    ds.StandaloneQuery.transaction.select(
                        ds.TransactionHeadlessQuery.signTransaction.args(
                            unsignedTransaction=unsigned_transaction.hex(),
                            signature=signature.hex(),
                        )
                    )
                )
            )
            result = session.execute(query)
            return bytes.fromhex(result["transaction"]["signTransaction"])

    def _save_transaction(self, signed_transaction: bytes, nonce) -> Transaction:
        transaction = Transaction()
        tx_id = hashlib.sha256(signed_transaction).hexdigest()
        transaction.tx_id = tx_id
        transaction.nonce = nonce
        transaction.signer = self.address
        transaction.payload = signed_transaction.hex()
        db.session.add(transaction)
        db.session.commit()
        return transaction

    def transfer_assets(
        self,
        time_stamp: datetime.datetime,
        nonce: int,
        recipients: typing.List[Recipient],
        memo: str,
        headless_url: str,
    ) -> Transaction:
        client = self._get_client(headless_url)
        with client as session:
            assert client.schema is not None
            ds = DSLSchema(client.schema)
            query = dsl_gql(
                DSLQuery(
                    ds.StandaloneQuery.actionTxQuery.args(
                        publicKey=self.public_key.hex(),
                        timestamp=time_stamp.isoformat(),
                        nonce=nonce,
                    ).select(
                        ds.ActionTxQuery.transferAssets.args(
                            sender=self.address, recipients=recipients, memo=memo
                        )
                    )
                )
            )
            result = session.execute(query)
            unsigned_transaction = bytes.fromhex(
                result["actionTxQuery"]["transferAssets"]
            )
            return self._sign_and_save(headless_url, unsigned_transaction, nonce)

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

    def query_transaction_result(self, headless_url: str, tx_id: str) -> str:
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
            transaction = db.session.query(Transaction).filter_by(tx_id=tx_id).one()
            transaction.tx_result = tx_status
            db.session.add(transaction)
            db.session.commit()
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

    async def stage_transactions_async(self, network_type: NetworkType):
        headless_urls = HEADLESS_URLS[network_type]
        transactions = Transaction.query.filter_by(tx_result=None).order_by(
            Transaction.nonce
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

    async def check_transaction_status_async(self, network_type: NetworkType):
        headless_url = MINER_URLS[network_type]
        transactions = Transaction.query.filter_by(tx_result=None).order_by(
            Transaction.nonce
        )
        await asyncio.gather(
            *[
                self.query_transaction_result_async(headless_url, transaction)
                for transaction in transactions
            ]
        )
        db.session.commit()

    async def query_transaction_result_async(
        self, headless_url: str, transaction: Transaction
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
            db.session.add(transaction)


signer = KmsWorldBossSigner(config.kms_key_id)
