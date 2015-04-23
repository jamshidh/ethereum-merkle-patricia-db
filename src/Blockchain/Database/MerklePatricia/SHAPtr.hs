{-# LANGUAGE OverloadedStrings #-}

module Blockchain.Database.MerklePatricia.SHAPtr (
  SHAPtr(..),
  emptyTriePtr,
  sha2SHAPtr
  ) where

import Control.Monad
import qualified Crypto.Hash.SHA3 as C
import Data.Binary
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Char8 as BC
import Data.Functor
import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Blockchain.Data.RLP
import Blockchain.ExtWord
import Blockchain.SHA

-- | Internal nodes are indexed in the underlying database by their 256-bit SHA3 hash.
-- This types represents said hash.
--
-- The stateRoot is of this type, 
-- (ie- the pointer to the full set of key/value pairs at a particular time in history), and
-- will be of interest if you need to refer to older or parallel version of the data.

newtype SHAPtr = SHAPtr B.ByteString deriving (Show, Eq, Read)

instance Pretty SHAPtr where
  pretty (SHAPtr x) = yellow $ text $ BC.unpack (B16.encode x)

instance Binary SHAPtr where
  put (SHAPtr x) = sequence_ $ put <$> B.unpack x
  get = SHAPtr <$> B.pack <$> replicateM 32 get

instance RLPSerializable SHAPtr where
    rlpEncode (SHAPtr x) = rlpEncode x
    rlpDecode x = SHAPtr $ rlpDecode x

-- | The stateRoot of the empty database.
emptyTriePtr::SHAPtr
emptyTriePtr = SHAPtr $ C.hash 256 $ rlpSerialize $ rlpEncode (0::Integer)

sha2SHAPtr::SHA->SHAPtr
sha2SHAPtr (SHA x) = SHAPtr $ B.pack $ word256ToBytes x

