
module Database.DBs (
  StateDB(..),
  SHAPtr(..)
  ) where

import Data.Binary
import qualified Data.ByteString as B
import Data.Functor
import qualified Database.LevelDB as DB

import Data.RLP

data StateDB =
  StateDB {
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
