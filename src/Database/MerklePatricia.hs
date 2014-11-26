{-# LANGUAGE OverloadedStrings #-}

module Database.MerklePatricia (
  --showAllKeyVal,
  SHAPtr(..),
--  NodeData(..),
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
import Numeric

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
newShortcut db key val = nodeData2NodeRef db $ ShortcutNodeData key val

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

--------------------

putKeyVal::MPDB->Key->Val->ResourceT IO MPDB
putKeyVal db key val = do
  p <- putNodeData db =<< putKV_NodeData db key val =<< getNodeData db (PtrRef $ stateRoot db)
  return db{stateRoot=p}

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
      
formatVal::Maybe RLPObject->Doc
formatVal Nothing = red $ text "NULL"
formatVal (Just x) = green $ pretty x
                
instance Pretty NodeData where
  pretty EmptyNodeData = text "    <EMPTY>"
  pretty (ShortcutNodeData s (Left p)) = text $ "    " ++ show (pretty s) ++ " -> " ++ show (pretty p)
  pretty (ShortcutNodeData s (Right val)) = text $ "    " ++ show (pretty s) ++ " -> " ++ show (green $ pretty val)
  pretty (FullNodeData cs val) = text "    val: " </> formatVal val </> text "\n        " </> vsep (showChoice <$> zip ([0..]::[Int]) cs)
    where
      showChoice (v, Just p) = blue (text $ showHex v "") </> text ": " </> green (pretty p)
      showChoice (v, Nothing) = blue (text $ showHex v "") </> text ": " </> red (text "NULL")



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
      getPtr o | B.length (rlpSerialize o) < 32 = SmallRef $ rlpSerialize o
      --getPtr o@(RLPArray [_, _]) = SmallRef $ rlpSerialize o
      getPtr p = PtrRef $ SHAPtr $ rlpDecode p
  rlpDecode x = error ("Missing case in rlpDecode for NodeData: " ++ show x)

