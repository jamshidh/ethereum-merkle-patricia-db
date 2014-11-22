{-# LANGUAGE OverloadedStrings #-}

module Database.MerklePatricia (
  --showAllKeyVal,
  SHAPtr(..),
  NodeData(..),
  MPDB(..),
  openMPDB,
  blankRoot,
  isBlankDB,
  getKeyVals,
  putKeyVal
  ) where

import Control.Monad.Trans.Resource
import qualified Crypto.Hash.SHA3 as C
import Data.Bits
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import Data.ByteString.Internal
import qualified Data.ByteString.Char8 as BC
import Data.Default
import Data.Function
import Data.Functor
import Data.List
import Data.Maybe
import qualified Data.NibbleString as N
import qualified Database.LevelDB as DB
--import qualified Data.Map as M
import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Data.RLP
import Database.MerklePatricia.MPDB

--import Debug.Trace

type Key = N.NibbleString
type Val = RLPObject

blankRoot::SHAPtr
blankRoot = SHAPtr (C.hash 256 "")

isBlankDB::SHAPtr->Bool
isBlankDB x | blankRoot == x = True
isBlankDB _ = False

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

nodeData2KeyVals::MPDB->NodeData->Key->ResourceT IO [(Key, Val)]
nodeData2KeyVals _ EmptyNodeData _ = return []
nodeData2KeyVals _ (ShortcutNodeData {nextNibbleString=s,nextVal=Right v}) key | key `N.isPrefixOf` s = return [(s, v)]
nodeData2KeyVals db ShortcutNodeData{nextNibbleString=s,nextVal=Left ref} key | key `N.isPrefixOf` s =
  fmap (prependToKey s) <$> nodeRef2KeyVals db ref ""
nodeData2KeyVals db ShortcutNodeData{nextNibbleString=s,nextVal=Left ref} key | s `N.isPrefixOf` key =
  fmap (prependToKey s) <$> nodeRef2KeyVals db ref (N.drop (N.length s) key)
nodeData2KeyVals _ ShortcutNodeData{} _ = return []
nodeData2KeyVals db (FullNodeData {choices=cs}) key = 
  if N.null key
  then concat <$> sequence [fmap (prependToKey (N.singleton nextN)) <$> nodeRef2KeyVals db ref "" | (nextN, Just ref) <- zip [0..] cs]
  else case cs!!fromIntegral (N.head key) of
    Just ref -> fmap (prependToKey $ N.singleton $ N.head key) <$> nodeRef2KeyVals db ref (N.tail key)
    Nothing -> return []

nodeRef2KeyVals::MPDB->NodeRef->Key->ResourceT IO [(Key, Val)]
nodeRef2KeyVals db ref key = do
  nodeData <- getNodeData db ref
  nodeData2KeyVals db nodeData key

getKeyVals::MPDB->Key->ResourceT IO [(Key, Val)]
getKeyVals db key = 
  nodeRef2KeyVals db (PtrRef $ stateRoot db) key

{-
let nodeData =case maybeNodeData of
                  Nothing -> error $ "Error calling getKeyVals, stateRoot doesn't exist: " ++ show (stateRoot db)
                  Just x -> x

      nextVals <- 
  case (N.null key, nodeData) of
    (True, FullNodeData{nodeVal = Just v}) -> return (("", v):nextVals)
    _ -> return nextVals
-}
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
list2Options start _ | start > 15 = error $ "value of 'start' in list2Option is greater than 15, it is: " ++ show start
list2Options start [] = replicate (fromIntegral $ 0x10 - start) Nothing
list2Options start ((firstNibble, firstPtr):rest) =
    replicate (fromIntegral $ firstNibble - start) Nothing ++ [Just firstPtr] ++ list2Options (firstNibble+1) rest

getCommonPrefix::Eq a=>[a]->[a]->([a], [a], [a])
getCommonPrefix (c1:rest1) (c2:rest2) | c1 == c2 = prefixTheCommonPrefix c1 (getCommonPrefix rest1 rest2)
                                      where
                                        prefixTheCommonPrefix c (p, x, y) = (c:p, x, y)
getCommonPrefix x y = ([], x, y)

newShortcut::MPDB->Key->Either NodeRef Val->ResourceT IO NodeRef
newShortcut _ key (Right val) | 32 > B.length bytes = return $ SmallRef bytes
                      where 
                        key' = termNibbleString2String True key
                        bytes = rlpSerialize $ RLPArray [rlpEncode $ BC.unpack key', val]
newShortcut db key val = PtrRef <$> putNodeData db (ShortcutNodeData key val)


putNodeData::MPDB->NodeData->ResourceT IO SHAPtr
putNodeData db nd = do
  let bytes = nodeDataSerialize nd
      ptr = C.hash 256 bytes
  DB.put (ldb db) def ptr bytes
  return $ SHAPtr ptr


data NodeRef = SmallRef B.ByteString | PtrRef SHAPtr deriving (Show)

instance Pretty NodeRef where
  pretty (SmallRef x) = green $ text $ BC.unpack $ B16.encode x
  pretty (PtrRef x) = green $ pretty x

createNodeRef::MPDB->NodeData->ResourceT IO NodeRef
createNodeRef db nd = do
  let bytes = nodeDataSerialize nd
  if B.length bytes < 32
    then return $ SmallRef bytes
    else do
      let ptr = C.hash 256 bytes
      DB.put (ldb db) def ptr bytes
      return $ PtrRef $ SHAPtr ptr

getNewNodeDataFromPut::MPDB->Key->Val->NodeData->ResourceT IO NodeData
getNewNodeDataFromPut _ key val EmptyNodeData = return $
  ShortcutNodeData key $ Right val

getNewNodeDataFromPut db key val (FullNodeData options nodeValue)
  | options `slotIsEmpty` N.head key = do
  tailNode <- newShortcut db (N.tail key) $ Right val
  return $ FullNodeData (replace options (N.head key) $ Just tailNode) nodeValue
getNewNodeDataFromPut db key val (FullNodeData options nodeValue) = do
  let Just conflictingNode = options!!fromIntegral (N.head key)
  --TODO- add nicer error message if stateRoot doesn't exist
  conflictingNodeData <- getNodeData db conflictingNode
  newNodeData <- getNewNodeDataFromPut db (N.tail key) val conflictingNodeData
  newNode <- PtrRef <$> putNodeData db newNodeData
  return $ FullNodeData (replace options (N.head key) $ Just newNode) nodeValue

getNewNodeDataFromPut _ key1 val (ShortcutNodeData key2 (Right _)) | key1 == key2 =
  return $ ShortcutNodeData key1 $ Right val
--getNewNodeDataFromPut _ key1 val (ShortcutNodeData key2 (Left _)) | key1 == key2 =
getNewNodeDataFromPut _ key1 _ (ShortcutNodeData key2 (Left _)) | key1 == key2 =
  error "getNewNodeDataFromPut not defined for shortcutnodedata with ptr"
--getNewNodeDataFromPut db key1 val1 (ShortcutNodeData key2 val2) | N.null key1 = do
getNewNodeDataFromPut _ key1 val1 (ShortcutNodeData k (Right _)) | N.null key1 = do
  return $ ShortcutNodeData k $ Right val1
  {-
  node1 <- putNodeData db $ ShortcutNodeData (N.drop (N.length key1) key2) val2
  let options = list2Options 0 [(N.head $ N.drop (N.length key1) key2, node1)]
  midNode <- putNodeData db $ FullNodeData options $ Just val1
  return $ ShortcutNodeData key1 $ Left midNode
-}
getNewNodeDataFromPut db key1 val1 (ShortcutNodeData key2 val2) | key1 `N.isPrefixOf` key2 = do
  node1 <- newShortcut db (N.drop (N.length key1) key2) val2
  let options = list2Options 0 [(N.head $ N.drop (N.length key1) key2, node1)]
  midNode <- createNodeRef db $ FullNodeData options $ Just val1
  return $ ShortcutNodeData key1 $ Left midNode
getNewNodeDataFromPut db key1 val1 (ShortcutNodeData key2 (Right val2)) | key2 `N.isPrefixOf` key1 = do
  node1 <- newShortcut db (N.drop (N.length key2) key1) $ Right val1
  let options = list2Options 0 [(N.head $ N.drop (N.length key2) key1, node1)]
  midNode <- createNodeRef db $ FullNodeData options $ Just val2
  return $ ShortcutNodeData key2 $ Left midNode
getNewNodeDataFromPut db key1 val1 (ShortcutNodeData key2 (Left ref)) | key2 `N.isPrefixOf` key1 = do
  nodeData <- getNodeData db ref
  newNodeData <- getNewNodeDataFromPut db (N.drop (N.length key2) key1) val1 nodeData 
  newNode <- createNodeRef db newNodeData
  return $ ShortcutNodeData key2 $ Left newNode
getNewNodeDataFromPut db key1 val1 (ShortcutNodeData key2 val2) | N.head key1 == N.head key2 = do
  node1 <- newShortcut db (N.pack $ tail suffix1) $ Right val1
  node2 <- newShortcut db (N.pack $ tail suffix2) val2
  let options = list2Options 0 (sortBy (compare `on` fst) [(head suffix1, node1), (head suffix2, node2)])
  midNode <- createNodeRef db $ FullNodeData options Nothing
  return $ ShortcutNodeData (N.pack commonPrefix) $ Left midNode
      where
        (commonPrefix, suffix1, suffix2) = getCommonPrefix (N.unpack key1) (N.unpack key2)
getNewNodeDataFromPut db key1 val1 (ShortcutNodeData key2 val2) = do
  tailNode1 <- newShortcut db (N.tail key1) $ Right val1
  tailNode2 <- newShortcut db (N.tail key2) val2
  return $ FullNodeData
      (list2Options 0 (sortBy (compare `on` fst) [(N.head key1, tailNode1), (N.head key2, tailNode2)]))
      Nothing

--getNewNodeDataFromPut _ key _ nd = error ("Missing case in getNewNodeDataFromPut: " ++ format nd ++ ", " ++ format key)

putKeyVal::MPDB->Key->Val->ResourceT IO MPDB
putKeyVal db key val = do
  curNodeData <- getNodeData db (PtrRef $ stateRoot db)
  nextNodeData <- getNewNodeDataFromPut db key val curNodeData
  let k = C.hash 256 $ nodeDataSerialize nextNodeData 
  DB.put (ldb db) def k $ nodeDataSerialize nextNodeData
  return db{stateRoot=SHAPtr k}

prependToKey::Key->(Key, Val)->(Key, Val)
prependToKey prefix (key, val) = (prefix `N.append` key, val)

data NodeData =
  EmptyNodeData |
  FullNodeData {
    --choices::M.Map N.Nibble (Maybe NodeRef),
    choices::[Maybe NodeRef],
    nodeVal::Maybe Val
  } |
  ShortcutNodeData {
    nextNibbleString::Key,
    nextVal::Either NodeRef Val
  } deriving Show

string2TermNibbleString::String->(Bool, N.NibbleString)
string2TermNibbleString [] = error "string2TermNibbleString called with empty String"
string2TermNibbleString (c:rest) = 
  (terminator, s)
  where
    w = c2w c
    (flags, extraNibble) = if w > 0xF then (w `shiftR` 4, 0xF .&. w) else (w, 0)
    terminator = flags `shiftR` 1 == 1
    oddLength = flags .&. 1 == 1
    s = if oddLength then N.OddNibbleString extraNibble (BC.pack rest) else N.EvenNibbleString (BC.pack rest)

termNibbleString2String::Bool->N.NibbleString->B.ByteString
termNibbleString2String terminator s = 
  case s of
    (N.EvenNibbleString s') -> B.singleton (extraNibble `shiftL` 4) `B.append` s'
    (N.OddNibbleString n rest) -> B.singleton (extraNibble `shiftL` 4 + n) `B.append` rest
  where
    {-
    nibbleString2String::N.NibbleString->String
    nibbleString2String (N.OddNibbleString c s) = w2c c:BC.unpack s
    nibbleString2String (N.EvenNibbleString s) = BC.unpack s
    -}
    extraNibble =
        (if terminator then 2 else 0) +
        (if odd $ N.length s then 1 else 0)

instance RLPSerializable NodeData where
  rlpEncode EmptyNodeData = error "rlpEncode should never be called on EmptyNodeData.  Use rlpSerialize instead."
  rlpEncode (FullNodeData {choices=cs, nodeVal=val}) = RLPArray ((encodeChoice <$> cs) ++ [encodeVal val])
    where
      encodeChoice::Maybe NodeRef->RLPObject
      encodeChoice Nothing = rlpEncode (0::Integer)
      encodeChoice (Just (PtrRef (SHAPtr x))) = rlpEncode x
      encodeChoice (Just (SmallRef o)) = rlpDeserialize o
      encodeVal::Maybe Val->RLPObject
      encodeVal Nothing = rlpEncode (0::Integer)
      encodeVal (Just x) = x
  rlpEncode (ShortcutNodeData {nextNibbleString=s, nextVal=val}) = 
    RLPArray[rlpEncode $ BC.unpack $ termNibbleString2String terminator s, encodeVal val] 
    where
      terminator = 
        case val of
          Left _ -> False
          Right _ -> True
      encodeVal::Either NodeRef Val->RLPObject
      encodeVal (Left (PtrRef x)) = rlpEncode x
      encodeVal (Left (SmallRef x)) = rlpEncode x
      encodeVal (Right x) = x



  rlpDecode (RLPArray [a, val]) = 
    if terminator
    then ShortcutNodeData s $ Right val
    else if B.length (rlpSerialize val) >= 32
         then ShortcutNodeData s (Left $ PtrRef $ SHAPtr (BC.pack $ rlpDecode val))
         else ShortcutNodeData s (Left $ SmallRef $ rlpDecode val)
    where
      (terminator, s) = string2TermNibbleString $ rlpDecode a
  rlpDecode (RLPArray x) | length x == 17 =
    FullNodeData (fmap getPtr <$> (\p -> case p of RLPScalar 0 -> Nothing; RLPString "" -> Nothing; _ -> Just p) <$> childPointers) val
    where
      childPointers = init x
      val = case last x of
        RLPScalar 0 -> Nothing
        RLPString "" -> Nothing
        x' -> Just x'
      getPtr::RLPObject->NodeRef
      getPtr o@(RLPArray [_, _]) = SmallRef $ rlpSerialize o
      getPtr p = PtrRef $ SHAPtr $ rlpDecode p
  rlpDecode x = error ("Missing case in rlpDecode for NodeData: " ++ show x)

