module Blockchain.Database.MerklePatricia (
  Key, Val, MPDB(..), SHAPtr(..),
  openMPDB, emptyTriePtr, sha2SHAPtr,
  putKeyVal, getAllKeyVals, getKeyVal, deleteKey, keyExists
  ) where

import Control.Monad.Trans.Resource
import Data.Functor ((<$>))
import Data.Maybe (isJust)
import qualified Data.NibbleString as N
import Blockchain.Database.MerklePatricia.Internal


-- | Adds a new key/value pair.
putKeyVal::MPDB -- ^ The object containing the current stateRoot.
         ->Key -- ^ Key of the data to be inserted.
         ->Val -- ^ Value of the new data
         ->ResourceT IO MPDB -- ^ The object containing the stateRoot to the data after the insert.
putKeyVal db = unsafePutKeyVal db . keyToSafeKey


-- | Retrieves all key/value pairs whose key starts with the given parameter.
getAllKeyVals::MPDB -- ^ Object containing the current stateRoot.
             ->ResourceT IO [(Key, Val)] -- ^ The requested data.
getAllKeyVals db = unsafeGetKeyVals db N.empty

getKeyVal::MPDB -> Key -> ResourceT IO (Maybe Val)
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
