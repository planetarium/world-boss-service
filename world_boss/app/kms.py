import datetime
import typing
import os

import backoff
import boto3
import base64
import ethereum_kms_signer  # type: ignore
import hashlib

import requests
from ethereum_kms_signer.spki import SPKIRecord  # type: ignore
from pyasn1.codec.der.decoder import decode as der_decode  # type: ignore
from pyasn1.codec.der.encoder import encode as der_encode  # type: ignore
from pyasn1.type.univ import Integer, SequenceOf  # type: ignore

from world_boss.app.enums import NetworkType
from world_boss.app.models import Transaction
from world_boss.app.orm import db


MINER_URLS: dict[NetworkType, str] = {
    NetworkType.MAIN: "http://9c-main-miner-3.nine-chronicles.com/graphql",
    NetworkType.INTERNAL: "http://a778316ca16af4065a02dc2753c1a0fc-1775306312.us-east-2.elb.amazonaws.com/graphql",
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
        query = """
        query($unsignedTransaction: String!, $signature: String!)
        {
            transaction {
                signTransaction(unsignedTransaction: $unsignedTransaction, signature: $signature)
            }
        }
        """
        variables = {
            "unsignedTransaction": unsigned_transaction.hex(),
            "signature": signature.hex(),
        }

        result = self._query(headless_url, query, variables)
        return bytes.fromhex(result["data"]["transaction"]["signTransaction"])

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.Timeout, requests.exceptions.ConnectionError),
        max_tries=5,
    )
    def _query(self, headless_url: str, query: str, variables: dict):
        result = requests.post(
            headless_url, json={"query": query, "variables": variables}
        )
        return result.json()

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
        recipients: typing.List[typing.Dict],
        memo: str,
        headless_url: str,
    ) -> Transaction:
        query = """
        query($publicKey: String!, $timeStamp: DateTimeOffset!, $nonce: Long, $sender: Address! $recipients: [RecipientsInputType!]!, $memo: String) {
          actionTxQuery(publicKey: $publicKey, timestamp: $timeStamp, nonce: $nonce) {
            transferAssets(sender: $sender, recipients: $recipients, memo: $memo)
          }
        }
            """
        variables = {
            "publicKey": self.public_key.hex(),
            "timeStamp": time_stamp.isoformat(),
            "nonce": nonce,
            "sender": self.address,
            "recipients": recipients,
            "memo": memo,
        }

        result = self._query(headless_url, query, variables)
        unsigned_transaction = bytes.fromhex(
            result["data"]["actionTxQuery"]["transferAssets"]
        )
        return self._sign_and_save(headless_url, unsigned_transaction, nonce)

    def stage_transactions(self, network_type: NetworkType) -> None:
        query = """
        mutation($payload: String!) {
          stageTransaction(payload: $payload)
        }
            """
        headless_urls = HEADLESS_URLS[network_type]
        transactions = (
            Transaction.query.filter_by(tx_result=None)
            .order_by(Transaction.nonce)
            .all()
        )
        for transaction in transactions:
            for headless_url in headless_urls:
                variables = {
                    "payload": transaction.payload,
                }
                result = self._query(headless_url, query, variables)
                print(result)

    def check_transaction_status(self, network_type: NetworkType):
        query = """
            query($txId: TxId!) {
              transaction {
                transactionResult(txId: $txId) {
                  blockHash
                  blockIndex
                  txStatus
                  exceptionName
                  exceptionMetadata
                }
              }
            }
            """
        headless_url = MINER_URLS[network_type]
        transactions = (
            Transaction.query.filter_by(tx_result=None)
            .order_by(Transaction.nonce)
            .all()
        )
        for transaction in transactions:
            variables = {
                "txId": transaction.tx_id,
            }
            result = self._query(headless_url, query, variables)
            tx_result = result["data"]["transaction"]["transactionResult"]
            tx_status = tx_result["txStatus"]
            transaction.tx_result = tx_status
            db.session.add(transaction)
        db.session.commit()


signer = KmsWorldBossSigner(os.environ["KMS_KEY_ID"])