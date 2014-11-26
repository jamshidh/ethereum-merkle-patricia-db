{-# LANGUAGE OverloadedStrings #-}

module Database.MerklePatricia (
  SHAPtr(..),
  MPDB(..),
  openMPDB,
  blankRoot,
  isBlankDB,
  getKeyVals,
  putKeyVal
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

import Data.RLP
import Database.MerklePatricia.MPDB
import Database.MerklePatricia.NodeData
import Database.MerklePatricia.SHAPtr

--import Debug.Trace




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
  then concat <$> sequence [fmap (prependToKey (N.singleton nextN)) <$> getKeyVals_NodeRef db ref "" | (nextN, Just ref) <- zip [0..] cs]
  else case cs!!fromIntegral (N.head key) of
    Just ref -> fmap (prependToKey $ N.singleton $ N.head key) <$> getKeyVals_NodeRef db ref (N.tail key)
    Nothing -> return []

----

getKeyVals_NodeRef::MPDB->NodeRef->Key->ResourceT IO [(Key, Val)]
getKeyVals_NodeRef db ref key = do
  nodeData <- getNodeData db ref
  getKeyVals_NodeData db nodeData key

getKeyVals::MPDB->Key->ResourceT IO [(Key, Val)]
getKeyVals db key = 
  getKeyVals_NodeRef db (PtrRef $ stateRoot db) key

------------------------------------

nodeDataSerialize::NodeData->B.ByteString
nodeDataSerialize EmptyNodeData = B.empty
nodeDataSerialize x = rlpSerialize $ rlpEncode x

slotIsEmpty::[Maybe NodeRef]->N.Nibble->Bool
slotIsEmpty [] _ = error ("slotIsEmpty was called for value greater than the size of the list")
slotIsEmpty (Nothing:_) 0 = True
slotIsEmpty _ 0 = False
slotIsEmpty (_:rest) n = slotIsEmpty rest (n-1)

replace::Integral i=>[a]->i->a->[a]
replace lst i newVal = left ++ [newVal] ++ right
            where
              (left, _:right) = splitAt (fromIntegral i) lst

list2Options::N.Nibble->[(N.Nibble, NodeRef)]->[Maybe NodeRef]
list2Options start [] = replicate (fromIntegral $ 0x10 - start) Nothing
list2Options start x | start > 15 =
  error $
  "value of 'start' in list2Option is greater than 15, it is: " ++ show start
  ++ ", second param is " ++ show x
list2Options start ((firstNibble, firstPtr):rest) =
    replicate (fromIntegral $ firstNibble - start) Nothing ++ [Just firstPtr] ++ list2Options (firstNibble+1) rest

getCommonPrefix::Eq a=>[a]->[a]->([a], [a], [a])
getCommonPrefix (c1:rest1) (c2:rest2) | c1 == c2 = prefixTheCommonPrefix c1 (getCommonPrefix rest1 rest2)
                                      where
                                        prefixTheCommonPrefix c (p, x, y) = (c:p, x, y)
getCommonPrefix x y = ([], x, y)

newShortcut::MPDB->Key->Either NodeRef Val->ResourceT IO NodeRef
newShortcut db key val = nodeData2NodeRef db $ ShortcutNodeData key val

putNodeData::MPDB->NodeData->ResourceT IO SHAPtr
putNodeData db nd = do
  let bytes = nodeDataSerialize nd
      ptr = C.hash 256 bytes
  DB.put (ldb db) def ptr bytes
  return $ SHAPtr ptr

-------------------------

nodeData2NodeRef::MPDB->NodeData->ResourceT IO NodeRef
nodeData2NodeRef db nodeData = do
  case rlpSerialize $ rlpEncode nodeData of
    bytes | B.length bytes < 32 -> return $ SmallRef bytes
    _ -> PtrRef <$> putNodeData db nodeData

putKV_NodeRef::MPDB->Key->Val->NodeRef->ResourceT IO NodeRef
putKV_NodeRef db key val nodeRef = do
  conflictingNodeData <- getNodeData db nodeRef
  newNodeData <- putKV_NodeData db key val conflictingNodeData
  nodeData2NodeRef db newNodeData

putKV_NodeData::MPDB->Key->Val->NodeData->ResourceT IO NodeData

----

putKV_NodeData _ key val EmptyNodeData = return $
  ShortcutNodeData key $ Right val

----

putKV_NodeData db key val (FullNodeData options nodeValue)
  | options `slotIsEmpty` N.head key = do
  tailNode <- newShortcut db (N.tail key) $ Right val
  return $ FullNodeData (replace options (N.head key) $ Just tailNode) nodeValue

putKV_NodeData db key val (FullNodeData options nodeValue) = do
  let Just conflictingNodeRef = options!!fromIntegral (N.head key)
  newNode <- putKV_NodeRef db (N.tail key) val conflictingNodeRef
  return $ FullNodeData (replace options (N.head key) $ Just newNode) nodeValue

----

putKV_NodeData _ key1 val (ShortcutNodeData key2 (Right _)) | key1 == key2 =
  return $ ShortcutNodeData key1 $ Right val

putKV_NodeData db key1 val (ShortcutNodeData key2 (Left ref)) | key1 == key2 = do
  newNodeRef <- putKV_NodeRef db key1 val ref
  return $ ShortcutNodeData key2 (Left newNodeRef)

putKV_NodeData db "" val1 (ShortcutNodeData key2 (Right val2)) = do
  newNodeRef <- newShortcut db (N.tail key2) $ Right val2
  return $ FullNodeData (list2Options 0 [(N.head $ key2, newNodeRef)]) $ Just val1

putKV_NodeData db key1 val1 (ShortcutNodeData key2 val2) | key1 `N.isPrefixOf` key2 = do
  tailNode <- newShortcut db (N.drop (N.length key1) key2) val2
  modifiedTailNode <- putKV_NodeRef db "" val1 tailNode
  return $ ShortcutNodeData key1 $ Left modifiedTailNode

putKV_NodeData db key1 val1 (ShortcutNodeData key2 (Right val2)) | key2 `N.isPrefixOf` key1 = do
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

putKeyVal::MPDB->Key->Val->ResourceT IO MPDB
putKeyVal db key val = do
  p <- putNodeData db =<< putKV_NodeData db key val =<< getNodeData db (PtrRef $ stateRoot db)
  return db{stateRoot=p}

--------------------


prependToKey::Key->(Key, Val)->(Key, Val)
prependToKey prefix (key, val) = (prefix `N.append` key, val)



