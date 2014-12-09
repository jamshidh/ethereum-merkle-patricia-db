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

-- | This is the database reference type, contianing both the handle to the underlying database, as well
-- as the stateRoot to the current tree holding the data.
-- 
-- The MPDB acts a bit like a traditional database handle, although because it contains the stateRoot,
-- many functions act by updating its value.  Because of this, it is recommended that this item be 
-- stored and modified within the state monad.
data MPDB =
  MPDB {
    ldb::DB.DB,
    stateRoot::SHAPtr
    }

-- | This function is used to create an MPDB object corresponding to the blank database.
-- After creation, the stateRoot can be changed to a previously saved version.
openMPDB::String -- ^ The filepath with the location of the underlying database.
        ->ResourceT IO MPDB
openMPDB path = do
  ldb' <- DB.open path def{DB.createIfMissing=True}
  DB.put ldb' def (BL.toStrict $ encode emptyTriePtr) B.empty
  return MPDB{ ldb=ldb', stateRoot=emptyTriePtr }
