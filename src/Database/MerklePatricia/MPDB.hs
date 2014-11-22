{-# LANGUAGE OverloadedStrings #-}

module Database.MerklePatricia.MPDB (
  MPDB(..),
  SHAPtr(..),
  openMPDB
  ) where

import Control.Monad.Trans.Resource
import qualified Crypto.Hash.SHA3 as C
import Data.Binary
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Data.Default
import Data.Functor
import qualified Database.LevelDB as DB

import Data.RLP

data MPDB =
  MPDB {
    ldb::DB.DB,
    stateRoot::SHAPtr
    }

newtype SHAPtr = SHAPtr B.ByteString deriving (Show, Eq)

instance Binary SHAPtr where
  put (SHAPtr x) = do
      sequence_ $ put <$> B.unpack x
  get = do
    error "get undefined for SHAPtr"

instance RLPSerializable SHAPtr where
    rlpEncode (SHAPtr x) = rlpEncode x
    rlpDecode x = SHAPtr $ rlpDecode x

blankRoot::SHAPtr
blankRoot = SHAPtr (C.hash 256 "")

openMPDB::String->ResourceT IO MPDB
openMPDB path = do
  ldb' <- DB.open path def{DB.createIfMissing=True}
  DB.put ldb' def (BL.toStrict $ encode blankRoot) B.empty
  return MPDB{ ldb=ldb', stateRoot=blankRoot }
