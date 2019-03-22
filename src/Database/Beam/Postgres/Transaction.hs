{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Database.Beam.Postgres.Transaction
    ( TransactionT, Transaction, transaction, savepoint
    , Run, none, one, only, many, some
    , Table, Select, Insert, Upsert, Update, Where, Projection
    , Q, QM, QE, QT, QIn, QOut, QEOut, QEq
    , select, count
    , insert, insertReturning
    , upsert, upsertReturning
    , save, update, updateReturning
    , forget, delete, deleteReturning
    , RecordNotFound (RecordNotFound)
    )
where

-- base ----------------------------------------------------------------------
import           Control.Applicative (Alternative)
import           Control.Applicative (liftA2)
import           Control.Exception (Exception)
import           Control.Monad (MonadPlus, unless, when)
import           Control.Monad.Fail (MonadFail)
import           Control.Monad.Fix (MonadFix)
import           Control.Monad.IO.Class (MonadIO)
import           Data.Functor.Identity (Identity (Identity), runIdentity)
import           Data.List.NonEmpty (NonEmpty, nonEmpty)
import           Data.Monoid (Monoid, mappend, mempty)
import           Data.Int (Int64)
import           Data.Proxy (Proxy (Proxy))
import           Data.Semigroup (Semigroup, (<>))
import           Data.Typeable (Typeable)
import           GHC.Generics (Generic, Generic1, Rep)


-- beam-core -----------------------------------------------------------------
import           Database.Beam.Backend (FromBackendRow)
import           Database.Beam.Query
                     ( QAssignment, QBaseScope, QExpr, QExprToIdentity
                     , QField, QGenExpr, QValueContext
                     , HaskellLiteralForQExpr
                     , Projectible
                     , SqlEq, SqlInsertValues, SqlValable, SqlValableTable
                     , (==.), aggregate_, countAll_, val_
                     )
import           Database.Beam.Query.Internal (QNested)
{-
import           Database.Beam.Query.Internal
                     ( AnyType
                     , ProjectibleInSelectSyntax
                     , ProjectibleWithPredicate
                     , QAssignment, QField, QNested
                     )
-}
import qualified Database.Beam.Query as B
import           Database.Beam.Schema
                     ( Beamable, DatabaseEntity, TableEntity, PrimaryKey, pk
                     )
import qualified Database.Beam.Schema.Tables as B


-- beam-postgres -------------------------------------------------------------
import           Database.Beam.Postgres (Postgres)
import qualified Database.Beam.Postgres.Conduit as P
import           Database.Beam.Postgres.Full (PgInsertOnConflict)
import qualified Database.Beam.Postgres.Full as P
import           Database.Beam.Postgres.Syntax (PostgresInaccessible)


-- conduit -------------------------------------------------------------------
import           Data.Conduit (ConduitT, await, connect)
import           Data.Conduit.List (consume)


-- layers --------------------------------------------------------------------
import           Control.Monad.Lift
                     ( MonadTrans, MInvariant, MFunctor
                     , MonadTransControl, LayerResult, LayerState
                     , suspend, resume, capture, extract
                     , defaultSuspend, defaultResume, defaultCapture
                     , defaultExtract
                     , MonadInner, liftI
                     , Iso1, Codomain1
                     )
import           Control.Monad.Lift.IO (liftIO)
import           Monad.Catch (MonadCatch, catch, handle)
import           Monad.Mask (MonadMask, mask)
import           Monad.Recover (onException)
import           Monad.ST (MonadST, newRef, readRef, writeRef)
import           Monad.Throw (MonadThrow, throw)
import           Monad.Try (MonadTry, finally)


-- postgresql-simple ---------------------------------------------------------
import           Database.PostgreSQL.Simple (Connection)
import           Database.PostgreSQL.Simple.Transaction
                     ( beginMode, defaultTransactionMode, rollback, commit
                     , newSavepoint, releaseSavepoint
                     , rollbackToAndReleaseSavepoint, isFailedTransactionError
                     )


-- transformers --------------------------------------------------------------
import           Control.Monad.Trans.Reader (ReaderT (ReaderT))


------------------------------------------------------------------------------
data RecordNotFound = RecordNotFound
  deriving (Eq, Ord, Read, Show, Generic, Typeable)
instance Exception RecordNotFound


------------------------------------------------------------------------------
bracket :: (MonadInner IO m, MonadST v m, MonadMask m, MonadTry m, MonadCatch m)
    => m a -> (a -> m ()) -> (a -> m ()) -> (a -> m b) -> m b
bracket acquire onFailure onSuccess run = mask $ \unmask -> do
    resource <- acquire
    ref <- newRef False
    unmask (run resource)
        `onException` do
            onFailure resource `finally` writeRef ref True
        `finally` do
            finished <- readRef ref
            unless finished $ onSuccess resource


------------------------------------------------------------------------------
transaction ::
    ( MonadInner IO m, MonadST v m, MonadMask m, MonadTry m, MonadCatch m
    )
    => TransactionT m a -> Connection -> m a
transaction (TransactionT (ReaderT f)) connection =
    bracket (liftI acquire) (liftI . rollback_) (liftI . commit) f
  where
    acquire = beginMode defaultTransactionMode connection >> pure connection
    rollback_ = handle (\(_ :: IOError) -> return ()) . rollback


------------------------------------------------------------------------------
savepoint ::
    ( MonadInner IO m, MonadST v m, MonadMask m, MonadCatch m, MonadTry m
    )
    => TransactionT m a -> TransactionT m a
savepoint (TransactionT (ReaderT f)) = TransactionT $ ReaderT go
  where
    go connection = bracket acquire onFailure onSuccess (const run)
      where
        run = f connection
        acquire = liftI $ newSavepoint connection
        onFailure = liftI . rollbackToAndReleaseSavepoint connection
        onSuccess savepoint_ = liftI $ do
            releaseSavepoint connection savepoint_ `catch` \e -> do
                when (isFailedTransactionError e) $
                    rollbackToAndReleaseSavepoint connection savepoint_
                throw e


------------------------------------------------------------------------------
newtype TransactionT m a = TransactionT (ReaderT Connection m a)
  deriving
    ( Functor, Applicative, Monad, Alternative, MonadPlus, MonadFix
    , Typeable, Generic, Generic1, MonadIO
    , MonadTrans, MInvariant, MFunctor, MonadFail
    )


------------------------------------------------------------------------------
instance Iso1 (TransactionT m) where
    type Codomain1 (TransactionT m) = ReaderT Connection m


------------------------------------------------------------------------------
instance MonadTransControl TransactionT where
    suspend = defaultSuspend
    resume = defaultResume
    capture = defaultCapture
    extract = defaultExtract


------------------------------------------------------------------------------
type instance LayerResult TransactionT = LayerResult (ReaderT Connection)
type instance LayerState TransactionT = LayerState (ReaderT Connection)


------------------------------------------------------------------------------
instance (Applicative m, Semigroup a) => Semigroup (TransactionT m a) where
    (<>) = liftA2 (<>)


------------------------------------------------------------------------------
instance (Applicative m, Monoid a) => Monoid (TransactionT m a) where
    mempty = pure mempty
    mappend = liftA2 mappend


------------------------------------------------------------------------------
type Transaction = TransactionT IO


------------------------------------------------------------------------------
type Run m f = forall a. (forall b. (ConduitT () a IO () -> IO b) -> IO b)
    -> m (f a)


------------------------------------------------------------------------------
none :: MonadInner IO m => Run m Proxy
none f = liftI (f (const (pure Proxy)))


------------------------------------------------------------------------------
one :: MonadInner IO m => Run m Maybe
one f = liftI (f (flip connect await))


------------------------------------------------------------------------------
only :: (MonadInner IO m, MonadThrow m) => Run m Identity
only f = fmap Identity <$> one f >>= go
  where
    go Nothing = throw RecordNotFound
    go (Just a) = pure a


------------------------------------------------------------------------------
many :: MonadInner IO m => Run m []
many f = liftI (f (flip connect consume))


------------------------------------------------------------------------------
some :: (MonadInner IO m, MonadThrow m) => Run m NonEmpty
some f = nonEmpty <$> many f >>= go
  where
    go Nothing = throw RecordNotFound
    go (Just a) = pure a


------------------------------------------------------------------------------
type Q db s t = QM db s (QT s t)


------------------------------------------------------------------------------
type QM db s = B.Q Postgres db s


------------------------------------------------------------------------------
type QE = QExpr Postgres


------------------------------------------------------------------------------
type QT s t = t (QE s)


------------------------------------------------------------------------------
type QIn p a = (a ~ HaskellLiteralForQExpr p, SqlValable p)


------------------------------------------------------------------------------
type QOut p a = (QEOut p, a ~ QExprToIdentity p, FromBackendRow Postgres a)


------------------------------------------------------------------------------
type QEOut = Projectible Postgres


------------------------------------------------------------------------------
type QEq s = SqlEq (QGenExpr QValueContext Postgres s)


------------------------------------------------------------------------------
type Table db table = DatabaseEntity Postgres db (TableEntity table)


------------------------------------------------------------------------------
type Projection table p = QT PostgresInaccessible table -> p


------------------------------------------------------------------------------
type Select db s = QM db s


------------------------------------------------------------------------------
type Insert table s = SqlInsertValues Postgres (table (QE s))


------------------------------------------------------------------------------
type Upsert = PgInsertOnConflict


------------------------------------------------------------------------------
type Update table = forall s. table (QField s) -> QAssignment Postgres s


------------------------------------------------------------------------------
type Where table = forall s. QT s table -> QE s Bool


------------------------------------------------------------------------------
select :: QOut p a => Run m f -> Select db QBaseScope p -> TransactionT m (f a)
select run q = TransactionT $ ReaderT $ \connection -> run $
    P.runSelect connection (B.select q)


------------------------------------------------------------------------------
count :: (MonadInner IO m, MonadThrow m, QEOut p)
    => Select db (QNested QBaseScope) p -> TransactionT m Word
count = fmap (fromIntegral . runIdentity) . select only
    . aggregate_ (const countAll_)


------------------------------------------------------------------------------
insert :: (MonadInner IO m, Beamable table)
    => Table db table -> Insert table s -> TransactionT m Int64
insert table as = TransactionT $ ReaderT $ \connection -> liftIO $
    P.runInsert connection $ P.insert table as u
  where
    u = P.onConflictDefault


------------------------------------------------------------------------------
insertReturning :: QOut p a
    => Run m f -> Table db table -> Insert table s -> Projection table p
    -> TransactionT m (f a)
insertReturning run table as f = TransactionT . ReaderT $ \connection -> run $
    P.runInsertReturning connection $ P.insertReturning table as u (Just f)
  where
    u = P.onConflictDefault


------------------------------------------------------------------------------
upsert :: (MonadInner IO m, Beamable table)
    => Table db table -> Insert table s -> Upsert table
    -> TransactionT m Int64
upsert table as u = TransactionT $ ReaderT $ \connection -> liftIO $
    P.runInsert connection $ P.insert table as u


------------------------------------------------------------------------------
upsertReturning :: QOut p a
    => Run m f -> Table db table -> Insert table s -> Projection table p
    -> Upsert table -> TransactionT m (f a)
upsertReturning run table as f u = TransactionT . ReaderT $ \connection -> run
    $ P.runInsertReturning connection $ P.insertReturning table as u (Just f)


------------------------------------------------------------------------------
type HasPrimaryKeyEquality table =
    ( Generic (PrimaryKey table
        (B.WithConstraint (B.HasSqlEqualityCheck Postgres)))
    , B.GFieldsFulfillConstraint
        (B.HasSqlEqualityCheck Postgres)
            (Rep (PrimaryKey table B.Exposed))
            (Rep (PrimaryKey table Identity))
            (Rep (PrimaryKey table
                (B.WithConstraint (B.HasSqlEqualityCheck Postgres))))
    )


------------------------------------------------------------------------------
save ::
    ( MonadInner IO m, B.Table table
    , SqlValableTable Postgres (PrimaryKey table)
    , SqlValableTable Postgres table
    , HasPrimaryKeyEquality table
    )
    => Table db table -> table Identity -> TransactionT m Int64
save table row = TransactionT $ ReaderT $ \connection -> liftIO $
    P.runUpdate connection $ B.save table row


------------------------------------------------------------------------------
update :: (MonadInner IO m, Beamable table)
    => Table db table -> Update table -> Where table -> TransactionT m Int64
update table u w = TransactionT $ ReaderT $ \connection -> liftIO $ do
    P.runUpdate connection $ B.update table u w


------------------------------------------------------------------------------
updateReturning :: QOut p a
    => Run m f -> Table db table -> Update table -> Where table
    -> Projection table p -> TransactionT m (f a)
updateReturning run table u w f = TransactionT . ReaderT $ \connection ->
    run $ P.runUpdateReturning connection $ P.updateReturning table u w f


------------------------------------------------------------------------------
forget ::
    ( MonadInner IO m, B.Table table
    , SqlValableTable Postgres (PrimaryKey table)
    , HasPrimaryKeyEquality table
    )
    => Table db table -> PrimaryKey table Identity -> TransactionT m Int64
forget table id_ = delete table ((==. val_ id_) . pk)


------------------------------------------------------------------------------
delete :: MonadInner IO m => Table db table -> Where table
    -> TransactionT m Int64
delete table w = TransactionT . ReaderT $ \connection -> liftIO $
    P.runDelete connection (B.delete table w)


------------------------------------------------------------------------------
deleteReturning :: QOut p a
    => Run m f -> Table db table -> Where table -> Projection table p
    -> TransactionT m (f a)
deleteReturning run table w f = TransactionT . ReaderT $ \connection -> run $
    P.runDeleteReturning connection $ P.deleteReturning table w f
