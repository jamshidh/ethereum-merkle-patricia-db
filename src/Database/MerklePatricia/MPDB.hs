{-# LANGUAGE OverloadedStrings #-}

module Database.MerklePatricia.MPDB (
  MPDB(..),
  openMPDB
  ) where

import Control.Monad.Trans.Resource
import Data.Binary
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Data.Default
import qualified Database.LevelDB as DB
--import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Database.MerklePatricia.SHAPtr

data MPDB =
  MPDB {
    ldb::DB.DB,
    stateRoot::SHAPtr
    }

openMPDB::String->ResourceT IO MPDB
openMPDB path = do
  ldb' <- DB.open path def{DB.createIfMissing=True}
  DB.put ldb' def (BL.toStrict $ encode blankRoot) B.empty
  return MPDB{ ldb=ldb', stateRoot=blankRoot }
