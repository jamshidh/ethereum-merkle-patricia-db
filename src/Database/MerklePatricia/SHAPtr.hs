{-# LANGUAGE OverloadedStrings #-}

module Database.MerklePatricia.SHAPtr (
  SHAPtr(..),
  isBlankDB,
  blankRoot
  ) where

import Control.Monad
import qualified Crypto.Hash.SHA3 as C
import Data.Binary
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Char8 as BC
import Data.Functor
import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Data.RLP

newtype SHAPtr = SHAPtr B.ByteString deriving (Show, Eq)

instance Pretty SHAPtr where
  pretty (SHAPtr x) = yellow $ text $ BC.unpack (B16.encode x)

instance Binary SHAPtr where
  put (SHAPtr x) = sequence_ $ put <$> B.unpack x
  get = SHAPtr <$> B.pack <$> replicateM 32 get

instance RLPSerializable SHAPtr where
    rlpEncode (SHAPtr x) = rlpEncode x
    rlpDecode x = SHAPtr $ rlpDecode x

blankRoot::SHAPtr
blankRoot = SHAPtr (C.hash 256 "")

isBlankDB::SHAPtr->Bool
isBlankDB x | blankRoot == x = True
isBlankDB _ = False

