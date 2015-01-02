{-# LANGUAGE OverloadedStrings #-}

-- | This is an implementation of the modified Merkle Patricia database described
--  in the Ethereum Yellowpaper (<http://gavwood.com/paper.pdf>).  This modified version
-- works like a canonical Merkle Patricia database, but includes certain optimizations.  In 
-- particular, a new type of "shortcut node" has been added to represent multiple traditional 
-- nodes that fall in a linear string (ie- a stretch of parent child nodes where no branch 
-- choices exist).
--
-- A Merkle Patricia Database effeciently retains its full history, and a snapshot of all key-value pairs
-- at a given time can be looked up using a "stateRoot" (a pointer to the root of the tree representing
-- that data).  Many of the functions in this module work by updating this object, so for anything more 
-- complicated than a single update, use of the state monad is recommended.
--
-- The underlying data is actually stored in LevelDB.  This module provides the logic to organize 
-- the key-value pairs in the appropriate Patricia Merkle Tree.

module Blockchain.Database.MerklePatricia (
  Key,
  Val,
  initializeBlank,
  putKeyVal,
  getKeyVals,
  deleteKey,
  MPDB(..),
  openMPDB,
  SHAPtr(..),
  emptyTriePtr,
  ) where

import Control.Monad.Trans.Resource
import qualified Crypto.Hash.SHA3 as C
import qualified Data.ByteString as B
import Data.Default
import Data.Function
import Data.Functor
import Data.List
import Data.Maybe
import qualified Data.NibbleString as N
import qualified Database.LevelDB as DB
import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Blockchain.Data.RLP
import Blockchain.Database.MerklePatricia.MPDB
import Blockchain.Database.MerklePatricia.NodeData
import Blockchain.Database.MerklePatricia.SHAPtr

--import Debug.Trace



initializeBlank::MPDB->ResourceT IO ()
initializeBlank db =
    let bytes = rlpSerialize $ rlpEncode (0::Integer)
    in
      DB.put (ldb db) def (C.hash 256 bytes) bytes


getNodeData::MPDB->NodeRef->ResourceT IO NodeData
getNodeData _ (SmallRef x) = return $ rlpDecode $ rlpDeserialize x
getNodeData db (PtrRef ptr@(SHAPtr p)) = do
  bytes <- fromMaybe (error $ "Missing SHAPtr in call to getNodeData: " ++ show (pretty ptr)) <$>
           DB.get (ldb db) def p
  return $ bytes2NodeData bytes
    where
      bytes2NodeData::B.ByteString->NodeData
      bytes2NodeData bytes | B.null bytes = EmptyNodeData
      bytes2NodeData bytes = rlpDecode $ rlpDeserialize bytes

-------------------------

getKeyVals_NodeData::MPDB->NodeData->Key->ResourceT IO [(Key, Val)]
getKeyVals_NodeData _ EmptyNodeData _ = return []
getKeyVals_NodeData _ (ShortcutNodeData {nextNibbleString=s,nextVal=Right v}) key | key `N.isPrefixOf` s = return [(s, v)]
getKeyVals_NodeData db ShortcutNodeData{nextNibbleString=s,nextVal=Left ref} key | key `N.isPrefixOf` s = 
  fmap (prependToKey s) <$> getKeyVals_NodeRef db ref ""
getKeyVals_NodeData db ShortcutNodeData{nextNibbleString=s,nextVal=Left ref} key | s `N.isPrefixOf` key = 
  fmap (prependToKey s) <$> getKeyVals_NodeRef db ref (N.drop (N.length s) key)
getKeyVals_NodeData _ ShortcutNodeData{} _ = return []
getKeyVals_NodeData db (FullNodeData {choices=cs}) key = 
  if N.null key
  then do
    partialKVs <- (sequence $ (\ref -> getKeyVals_NodeRef db ref "") <$> cs)::ResourceT IO [[(Key, Val)]]
    return $ concat $ (\(nibble, kvs) -> prependToKey (N.singleton nibble) <$> kvs) <$> zip [0..] partialKVs
  else case cs!!fromIntegral (N.head key) of
    x | x == emptyRef -> return []
    ref -> fmap (prependToKey $ N.singleton $ N.head key) <$> getKeyVals_NodeRef db ref (N.tail key)

----

getKeyVals_NodeRef::MPDB->NodeRef->Key->ResourceT IO [(Key, Val)]
getKeyVals_NodeRef db ref key = do
  nodeData <- getNodeData db ref
  getKeyVals_NodeData db nodeData key

-- | Retrieves all key/value pairs whose key starts with the given parameter.


getKeyVals::MPDB -- ^ Object containing the current stateRoot.
          ->Key -- ^ The partial key (the query will return any key that is prefixed by this value)
          ->ResourceT IO [(Key, Val)] -- ^ The requested data.
getKeyVals db = 
  getKeyVals_NodeRef db (PtrRef $ stateRoot db)

------------------------------------

slotIsEmpty::[NodeRef]->N.Nibble->Bool
slotIsEmpty [] _ = error "slotIsEmpty was called for value greater than the size of the list"
slotIsEmpty (x:_) 0 | x == emptyRef = True
slotIsEmpty _ 0 = False
slotIsEmpty (_:rest) n = slotIsEmpty rest (n-1)

replace::Integral i=>[a]->i->a->[a]
replace lst i newVal = left ++ [newVal] ++ right
            where
              (left, _:right) = splitAt (fromIntegral i) lst

list2Options::N.Nibble->[(N.Nibble, NodeRef)]->[NodeRef]
list2Options start [] = replicate (fromIntegral $ 0x10 - start) emptyRef
list2Options start x | start > 15 =
  error $
  "value of 'start' in list2Option is greater than 15, it is: " ++ show start
  ++ ", second param is " ++ show x
list2Options start ((firstNibble, firstPtr):rest) =
    replicate (fromIntegral $ firstNibble - start) emptyRef ++ [firstPtr] ++ list2Options (firstNibble+1) rest

options2List::[NodeRef]->[(N.Nibble, NodeRef)]
options2List theList = filter ((/= emptyRef) . snd) $ zip [0..] theList 

getCommonPrefix::Eq a=>[a]->[a]->([a], [a], [a])
getCommonPrefix (c1:rest1) (c2:rest2) | c1 == c2 = prefixTheCommonPrefix c1 (getCommonPrefix rest1 rest2)
                                      where
                                        prefixTheCommonPrefix c (p, x, y) = (c:p, x, y)
getCommonPrefix x y = ([], x, y)

newShortcut::MPDB->Key->Either NodeRef Val->ResourceT IO NodeRef
newShortcut _ key (Left ref) | N.null key = return ref
newShortcut db key val = nodeData2NodeRef db $ ShortcutNodeData key val

putNodeData::MPDB->NodeData->ResourceT IO SHAPtr
putNodeData db nd = do
  let bytes = rlpSerialize $ rlpEncode nd
      ptr = C.hash 256 bytes
  DB.put (ldb db) def ptr bytes
  return $ SHAPtr ptr

-------------------------

nodeData2NodeRef::MPDB->NodeData->ResourceT IO NodeRef
nodeData2NodeRef db nodeData =
  case rlpSerialize $ rlpEncode nodeData of
    bytes | B.length bytes < 32 -> return $ SmallRef bytes
    _ -> PtrRef <$> putNodeData db nodeData

putKV_NodeRef::MPDB->Key->Val->NodeRef->ResourceT IO NodeRef
putKV_NodeRef db key val nodeRef = do
  nodeData <- getNodeData db nodeRef
  newNodeData <- putKV_NodeData db key val nodeData
  nodeData2NodeRef db newNodeData

putKV_NodeData::MPDB->Key->Val->NodeData->ResourceT IO NodeData

----

putKV_NodeData _ key val EmptyNodeData = return $
  ShortcutNodeData key $ Right val

----

putKV_NodeData db key val (FullNodeData options nodeValue)
  | options `slotIsEmpty` N.head key = do
  tailNode <- newShortcut db (N.tail key) $ Right val
  return $ FullNodeData (replace options (N.head key) tailNode) nodeValue

putKV_NodeData db key val (FullNodeData options nodeValue) = do
  let conflictingNodeRef = options!!fromIntegral (N.head key)
  newNode <- putKV_NodeRef db (N.tail key) val conflictingNodeRef
  return $ FullNodeData (replace options (N.head key) newNode) nodeValue

----

putKV_NodeData _ key1 val (ShortcutNodeData key2 (Right _)) | key1 == key2 =
  return $ ShortcutNodeData key1 $ Right val

putKV_NodeData db key1 val (ShortcutNodeData key2 (Left ref)) | key1 == key2 = do
  newNodeRef <- putKV_NodeRef db key1 val ref
  return $ ShortcutNodeData key2 (Left newNodeRef)

putKV_NodeData db "" val1 (ShortcutNodeData key2 val2) = do
  newNodeRef <- newShortcut db (N.tail key2) val2
  return $ FullNodeData (list2Options 0 [(N.head key2, newNodeRef)]) $ Just val1

putKV_NodeData db key1 val1 (ShortcutNodeData key2 val2) | key1 `N.isPrefixOf` key2 = do
  tailNode <- newShortcut db (N.drop (N.length key1) key2) val2
  modifiedTailNode <- putKV_NodeRef db "" val1 tailNode
  return $ ShortcutNodeData key1 $ Left modifiedTailNode

putKV_NodeData db key1 val1 (ShortcutNodeData key2 (Right val2)) | key2 `N.isPrefixOf` key1 =
  putKV_NodeData db key2 val2 (ShortcutNodeData key1 $ Right val1)

putKV_NodeData db key1 val1 (ShortcutNodeData key2 (Left ref)) | key2 `N.isPrefixOf` key1 = do
  newNode <- putKV_NodeRef db (N.drop (N.length key2) key1) val1 ref
  return $ ShortcutNodeData key2 $ Left newNode

putKV_NodeData db key1 val1 (ShortcutNodeData key2 val2) | N.head key1 == N.head key2 = do
  nodeAfterCommonBeforePut <- newShortcut db (N.pack suffix2) val2
  nodeAfterCommon <- putKV_NodeRef db (N.pack suffix1) val1 nodeAfterCommonBeforePut
  return $ ShortcutNodeData (N.pack commonPrefix) $ Left nodeAfterCommon
      where
        (commonPrefix, suffix1, suffix2) = getCommonPrefix (N.unpack key1) (N.unpack key2)

putKV_NodeData db key1 val1 (ShortcutNodeData key2 val2) = do
  tailNode1 <- newShortcut db (N.tail key1) $ Right val1
  tailNode2 <- newShortcut db (N.tail key2) val2
  return $ FullNodeData
      (list2Options 0 (sortBy (compare `on` fst) [(N.head key1, tailNode1), (N.head key2, tailNode2)]))
      Nothing

-- | Adds a new key/value pair.
putKeyVal::MPDB -- ^ The object containing the current stateRoot.
         ->Key -- ^ Key of the data to be inserted.
         ->Val -- ^ Value of the new data
         ->ResourceT IO MPDB -- ^ The object containing the stateRoot to the data after the insert.
--putKeyVal db key val | trace ("^^^^^^^^^^putKeyVal: key = " ++ show (pretty key) ++ ", val = " ++ show (pretty val)) False = undefined
putKeyVal db key val = do
  p <- putNodeData db =<< putKV_NodeData db key val =<< getNodeData db (PtrRef $ stateRoot db)
  return db{stateRoot=p}

--------------------

--The "simplify" functions are only used to canonicalize the DB after a delete.
--We need to concatinate ShortcutNodeData links, convert FullNodeData to ShortcutNodeData when possible, etc.

--Important note- this function should only apply to immediate items, and not recurse deep into the database (ie- by
--simplifying all options in a FullNodeData, etc).  Failure to adhere will result in a performance nightmare!
--Any delete could result in a full read through the whole database.  The delete function only will "break" the canonical structure locally, so deep recursion isn't required.

simplify_NodeRef::MPDB->NodeRef->ResourceT IO NodeRef
simplify_NodeRef db ref = nodeData2NodeRef db =<< simplify_NodeData db =<< getNodeData db ref

----

simplify_NodeData::MPDB->NodeData->ResourceT IO NodeData
simplify_NodeData _ EmptyNodeData = return EmptyNodeData
simplify_NodeData db nd@(ShortcutNodeData key (Left ref)) = do
  refNodeData <- simplify_NodeData db =<< getNodeData db ref
  case refNodeData of
    (ShortcutNodeData key2 v2) -> return $ ShortcutNodeData (key `N.append` key2) v2
    _ -> return nd
simplify_NodeData db (FullNodeData options Nothing) = do
    simplifiedOptions <- sequence $ simplify_NodeRef db <$> options

    case options2List simplifiedOptions of
      [(n, nodeRef)] ->
          simplify_NodeData db $ ShortcutNodeData (N.singleton n) $ Left nodeRef
      _ -> return $ FullNodeData simplifiedOptions Nothing
simplify_NodeData _ x = return x

----------

--TODO- This is looking like a lift, I probably should make NodeRef some sort of Monad....

deleteKey_NodeRef::MPDB->Key->NodeRef->ResourceT IO NodeRef
deleteKey_NodeRef db key nodeRef =
  nodeData2NodeRef db =<< deleteKey_NodeData db key =<< getNodeData db nodeRef

----

deleteKey_NodeData::MPDB->Key->NodeData->ResourceT IO NodeData

deleteKey_NodeData _ _ EmptyNodeData = return EmptyNodeData


deleteKey_NodeData _ key1 (ShortcutNodeData key2 (Right _)) | key2 == key1 = return EmptyNodeData
deleteKey_NodeData _ _ nd@(ShortcutNodeData _ (Right _)) = return nd
deleteKey_NodeData db key1 (ShortcutNodeData key2 (Left ref)) | key2 `N.isPrefixOf` key1 = do
  newNodeRef <- deleteKey_NodeRef db (N.drop (N.length key2) key1) ref
  simplify_NodeData db $ ShortcutNodeData key2 $ Left newNodeRef
deleteKey_NodeData _ _ nd@(ShortcutNodeData _ (Left _)) = return nd


deleteKey_NodeData _ "" (FullNodeData options _) =
    return $ FullNodeData options Nothing
deleteKey_NodeData _ key nd@(FullNodeData options _) | options `slotIsEmpty` N.head key =
                                                         return nd
deleteKey_NodeData db key (FullNodeData options val) = do
    let nodeRef = options!!fromIntegral (N.head key)
    newNodeRef <- deleteKey_NodeRef db (N.tail key) nodeRef
    let newOptions = replace options (N.head key) newNodeRef
    simplify_NodeData db $ FullNodeData newOptions val

-------------


-- | Deletes a key (and its corresponding data) from the database.
-- 
-- Note that the key/value pair will still be present in the history, and can be accessed
-- by using an older 'MPDB' object.

deleteKey::MPDB -- ^ The object containing the current stateRoot.
         ->Key -- ^ The key to be deleted.
         ->ResourceT IO MPDB -- ^ The object containing the stateRoot to the data after the delete.
deleteKey db key = do
  p <- putNodeData db =<< deleteKey_NodeData db key =<< getNodeData db (PtrRef $ stateRoot db)
  return db{stateRoot=p}


prependToKey::Key->(Key, Val)->(Key, Val)
prependToKey prefix (key, val) = (prefix `N.append` key, val)



