{-# LANGUAGE OverloadedStrings #-}

module Database.MerklePatricia.NodeData (
  Key,
  Val,
  NodeData(..),
  NodeRef(..)
  ) where

import Data.Bits
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import Data.ByteString.Internal
import qualified Data.ByteString.Char8 as BC
import Data.Functor
import qualified Data.NibbleString as N
import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))
import Numeric

import Data.RLP
--import Database.MerklePatricia.MPDB
import Database.MerklePatricia.SHAPtr

--import Debug.Trace

-------------------------

type Key = N.NibbleString
type Val = RLPObject

-------------------------

data NodeRef = SmallRef B.ByteString | PtrRef SHAPtr deriving (Show)

instance Pretty NodeRef where
  pretty (SmallRef x) = green $ text $ BC.unpack $ B16.encode x
  pretty (PtrRef x) = green $ pretty x

-------------------------

data NodeData =
  EmptyNodeData |
  FullNodeData {
    -- Why not make choices a map (choices::M.Map N.Nibble NodeRef)?  Because this type tends to be created 
    -- more than items are looked up in it....  It would actually slow things down to use it.
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
