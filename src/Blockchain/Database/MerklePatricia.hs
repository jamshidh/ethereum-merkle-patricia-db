-- | This is an implementation of the modified Merkle Patricia database
-- described in the Ethereum Yellowpaper
-- (<http://gavwood.com/paper.pdf>).  This modified version works like a
-- canonical Merkle Patricia database, but includes certain
-- optimizations.  In particular, a new type of "shortcut node" has been
-- added to represent multiple traditional nodes that fall in a linear
-- string (ie- a stretch of parent child nodes where no branch choices
-- exist).
--
-- A Merkle Patricia Database effeciently retains its full history, and a
-- snapshot of all key-value pairs at a given time can be looked up using
-- a "stateRoot" (a pointer to the root of the tree representing that
-- data).  Many of the functions in this module work by updating this
-- object, so for anything more complicated than a single update, use of
-- the state monad is recommended.
--
-- The underlying data is actually stored in LevelDB.  This module
-- provides the logic to organize the key-value pairs in the appropriate
-- Patricia Merkle Tree.

module Blockchain.Database.MerklePatricia (
  Key, Val, MPDB(..), SHAPtr(..),
  openMPDB, emptyTriePtr, sha2SHAPtr,
  putKeyVal, getKeyVal, deleteKey, keyExists,
  initializeBlank
  ) where

import qualified Crypto.Hash.SHA3 as SHA3
import Control.Monad.Trans.Resource
import Data.Default
import Data.Functor ((<$>))
import Data.Maybe (isJust)
import qualified Database.LevelDB as DB

import Blockchain.Data.RLP
import Blockchain.Database.MerklePatricia.Internal


-- | Adds a new key/value pair.
putKeyVal::MPDB -- ^ The object containing the current stateRoot.
         ->Key -- ^ Key of the data to be inserted.
         ->Val -- ^ Value of the new data
         ->ResourceT IO MPDB -- ^ The object containing the stateRoot to the data after the insert.
putKeyVal db = unsafePutKeyVal db . keyToSafeKey

-- | Retrieves all key/value pairs whose key starts with the given parameter.
getKeyVal::MPDB -- ^ Object containing the current stateRoot.
         -> Key -- ^ Key of the data to be inserted.
         -> ResourceT IO (Maybe Val) -- ^ The requested value.
getKeyVal db key = do
  vals <- unsafeGetKeyVals db (keyToSafeKey key)
  return $
    if not (null vals)
    then Just $ snd (head vals)
         -- Since we hash the keys, it's impossible
         -- for vals to have more than one item
    else Nothing

-- | Deletes a key (and its corresponding data) from the database.
-- 
-- Note that the key/value pair will still be present in the history, and
-- can be accessed by using an older 'MPDB' object.
deleteKey::MPDB -- ^ The object containing the current stateRoot.
         ->Key -- ^ The key to be deleted.
         ->ResourceT IO MPDB -- ^ The object containing the stateRoot to the data after the delete.
deleteKey db = unsafeDeleteKey db . keyToSafeKey

-- | Returns True is a key exists.
keyExists::MPDB -- ^ The object containing the current stateRoot.
         ->Key -- ^ The key to be deleted.
         ->ResourceT IO Bool -- ^ True if the key exists
keyExists db key = isJust <$> getKeyVal db key


-- | Initialize the DB by adding a blank stateroot.
initializeBlank::MPDB -- ^ The object containing the current stateRoot.
               ->ResourceT IO ()
initializeBlank db =
    let bytes = rlpSerialize $ rlpEncode (0::Integer)
    in
      DB.put (ldb db) def (SHA3.hash 256 bytes) bytes

