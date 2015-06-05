module Blockchain.Database.MerklePatricia.Diff (dbDiff, DiffOp(..)) where

import Blockchain.Database.MerklePatricia.NodeData
import Blockchain.Database.MerklePatricia.Internal

import Control.Monad
import Control.Monad.Trans.Class
import Control.Monad.Trans.Reader
import Control.Monad.Trans.Resource
import Data.Function
import qualified Data.NibbleString as N

-- Probably the entire MPDB system ought to be in this monad
type MPReaderM = ReaderT MPDB (ResourceT IO)

data MPChoice = Data NodeData | Ref NodeRef | Value Val | None deriving (Eq)

node :: MPChoice -> MPReaderM NodeData
node (Data nd) = return nd
node (Ref nr) = do
  derefNode <- asks getNodeData 
  lift $ derefNode nr
node _ = return EmptyNodeData

simplify :: NodeData -> [MPChoice]
simplify EmptyNodeData = replicate 17 None -- 17: not a mistake
simplify FullNodeData{ choices = ch, nodeVal = v } =
  maybe None Value v : map Ref ch
simplify n@ShortcutNodeData{ nextNibbleString = k, nextVal = v } = None : delta h
  where
    delta m =
      let pre = replicate m None
          post = replicate (16 - m - 1) None
      in pre ++ [x] ++ post
    x | N.null t  = either Ref Value v
      | otherwise = Data n{ nextNibbleString = t }
    (h,t) = (fromIntegral $ N.head k, N.tail k)

enter :: MPChoice -> MPReaderM [MPChoice]
enter = liftM simplify . node

data DiffOp =
  Create {key::[N.Nibble], val::Val} |
  Update {key::[N.Nibble], oldVal::Val, newVal::Val} |
  Delete {key::[N.Nibble]}
  deriving (Show, Eq)

diffChoice :: Maybe N.Nibble -> MPChoice -> MPChoice -> MPReaderM [DiffOp]
diffChoice n ch1 ch2 = case (ch1, ch2) of
  (None, Value v) -> return [Create sn v]
  (Value _, None) -> return [Delete sn]
  (Value v1, Value v2)
    | v1 /= v2     -> return [Update sn v1 v2]
  _ | ch1 == ch2   -> return []
    | otherwise   -> pRecurse ch1 ch2
  where
    sn = maybe [] (:[]) n
    prefix =
      let prepend n' op = op{key = n':(key op)}
      in map (maybe id prepend n)
    pRecurse = liftM prefix .* recurse

diffChoices :: [MPChoice] -> [MPChoice] -> MPReaderM [DiffOp]
diffChoices =
  liftM concat .* sequence .* zipWith3 diffChoice maybeNums 
  where maybeNums = Nothing : map Just [0..]

recurse :: MPChoice -> MPChoice -> MPReaderM [DiffOp]
recurse = join .* (liftM2 diffChoices `on` enter)

infixr 9 .*
(.*) :: (c -> d) -> (a -> b -> c) -> (a -> b -> d)
(.*) = (.) . (.)

diff :: NodeRef -> NodeRef -> MPReaderM [DiffOp]
diff = recurse `on` Ref

dbDiff :: MPDB -> SHAPtr -> SHAPtr -> ResourceT IO [DiffOp]
dbDiff db root1 root2 = runReaderT ((diff `on` PtrRef) root1 root2) db
