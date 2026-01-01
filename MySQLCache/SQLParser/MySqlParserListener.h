
// Generated from MySqlParser.g4 by ANTLR 4.13.0

#pragma once


#include "antlr4-runtime.h"
#include "MySqlParser.h"


/**
 * This interface defines an abstract listener for a parse tree produced by MySqlParser.
 */
class  MySqlParserListener : public antlr4::tree::ParseTreeListener {
public:

  virtual void enterRoot(MySqlParser::RootContext *ctx) = 0;
  virtual void exitRoot(MySqlParser::RootContext *ctx) = 0;

  virtual void enterSqlStatements(MySqlParser::SqlStatementsContext *ctx) = 0;
  virtual void exitSqlStatements(MySqlParser::SqlStatementsContext *ctx) = 0;

  virtual void enterSqlStatement(MySqlParser::SqlStatementContext *ctx) = 0;
  virtual void exitSqlStatement(MySqlParser::SqlStatementContext *ctx) = 0;

  virtual void enterEmptyStatement_(MySqlParser::EmptyStatement_Context *ctx) = 0;
  virtual void exitEmptyStatement_(MySqlParser::EmptyStatement_Context *ctx) = 0;

  virtual void enterDdlStatement(MySqlParser::DdlStatementContext *ctx) = 0;
  virtual void exitDdlStatement(MySqlParser::DdlStatementContext *ctx) = 0;

  virtual void enterDmlStatement(MySqlParser::DmlStatementContext *ctx) = 0;
  virtual void exitDmlStatement(MySqlParser::DmlStatementContext *ctx) = 0;

  virtual void enterTransactionStatement(MySqlParser::TransactionStatementContext *ctx) = 0;
  virtual void exitTransactionStatement(MySqlParser::TransactionStatementContext *ctx) = 0;

  virtual void enterReplicationStatement(MySqlParser::ReplicationStatementContext *ctx) = 0;
  virtual void exitReplicationStatement(MySqlParser::ReplicationStatementContext *ctx) = 0;

  virtual void enterPreparedStatement(MySqlParser::PreparedStatementContext *ctx) = 0;
  virtual void exitPreparedStatement(MySqlParser::PreparedStatementContext *ctx) = 0;

  virtual void enterCompoundStatement(MySqlParser::CompoundStatementContext *ctx) = 0;
  virtual void exitCompoundStatement(MySqlParser::CompoundStatementContext *ctx) = 0;

  virtual void enterAdministrationStatement(MySqlParser::AdministrationStatementContext *ctx) = 0;
  virtual void exitAdministrationStatement(MySqlParser::AdministrationStatementContext *ctx) = 0;

  virtual void enterUtilityStatement(MySqlParser::UtilityStatementContext *ctx) = 0;
  virtual void exitUtilityStatement(MySqlParser::UtilityStatementContext *ctx) = 0;

  virtual void enterCreateDatabase(MySqlParser::CreateDatabaseContext *ctx) = 0;
  virtual void exitCreateDatabase(MySqlParser::CreateDatabaseContext *ctx) = 0;

  virtual void enterCreateEvent(MySqlParser::CreateEventContext *ctx) = 0;
  virtual void exitCreateEvent(MySqlParser::CreateEventContext *ctx) = 0;

  virtual void enterCreateIndex(MySqlParser::CreateIndexContext *ctx) = 0;
  virtual void exitCreateIndex(MySqlParser::CreateIndexContext *ctx) = 0;

  virtual void enterCreateLogfileGroup(MySqlParser::CreateLogfileGroupContext *ctx) = 0;
  virtual void exitCreateLogfileGroup(MySqlParser::CreateLogfileGroupContext *ctx) = 0;

  virtual void enterCreateProcedure(MySqlParser::CreateProcedureContext *ctx) = 0;
  virtual void exitCreateProcedure(MySqlParser::CreateProcedureContext *ctx) = 0;

  virtual void enterCreateFunction(MySqlParser::CreateFunctionContext *ctx) = 0;
  virtual void exitCreateFunction(MySqlParser::CreateFunctionContext *ctx) = 0;

  virtual void enterCreateRole(MySqlParser::CreateRoleContext *ctx) = 0;
  virtual void exitCreateRole(MySqlParser::CreateRoleContext *ctx) = 0;

  virtual void enterCreateServer(MySqlParser::CreateServerContext *ctx) = 0;
  virtual void exitCreateServer(MySqlParser::CreateServerContext *ctx) = 0;

  virtual void enterCopyCreateTable(MySqlParser::CopyCreateTableContext *ctx) = 0;
  virtual void exitCopyCreateTable(MySqlParser::CopyCreateTableContext *ctx) = 0;

  virtual void enterQueryCreateTable(MySqlParser::QueryCreateTableContext *ctx) = 0;
  virtual void exitQueryCreateTable(MySqlParser::QueryCreateTableContext *ctx) = 0;

  virtual void enterColumnCreateTable(MySqlParser::ColumnCreateTableContext *ctx) = 0;
  virtual void exitColumnCreateTable(MySqlParser::ColumnCreateTableContext *ctx) = 0;

  virtual void enterCreateTablespaceInnodb(MySqlParser::CreateTablespaceInnodbContext *ctx) = 0;
  virtual void exitCreateTablespaceInnodb(MySqlParser::CreateTablespaceInnodbContext *ctx) = 0;

  virtual void enterCreateTablespaceNdb(MySqlParser::CreateTablespaceNdbContext *ctx) = 0;
  virtual void exitCreateTablespaceNdb(MySqlParser::CreateTablespaceNdbContext *ctx) = 0;

  virtual void enterCreateTrigger(MySqlParser::CreateTriggerContext *ctx) = 0;
  virtual void exitCreateTrigger(MySqlParser::CreateTriggerContext *ctx) = 0;

  virtual void enterWithClause(MySqlParser::WithClauseContext *ctx) = 0;
  virtual void exitWithClause(MySqlParser::WithClauseContext *ctx) = 0;

  virtual void enterCommonTableExpressions(MySqlParser::CommonTableExpressionsContext *ctx) = 0;
  virtual void exitCommonTableExpressions(MySqlParser::CommonTableExpressionsContext *ctx) = 0;

  virtual void enterCteName(MySqlParser::CteNameContext *ctx) = 0;
  virtual void exitCteName(MySqlParser::CteNameContext *ctx) = 0;

  virtual void enterCteColumnName(MySqlParser::CteColumnNameContext *ctx) = 0;
  virtual void exitCteColumnName(MySqlParser::CteColumnNameContext *ctx) = 0;

  virtual void enterCreateView(MySqlParser::CreateViewContext *ctx) = 0;
  virtual void exitCreateView(MySqlParser::CreateViewContext *ctx) = 0;

  virtual void enterCreateDatabaseOption(MySqlParser::CreateDatabaseOptionContext *ctx) = 0;
  virtual void exitCreateDatabaseOption(MySqlParser::CreateDatabaseOptionContext *ctx) = 0;

  virtual void enterCharSet(MySqlParser::CharSetContext *ctx) = 0;
  virtual void exitCharSet(MySqlParser::CharSetContext *ctx) = 0;

  virtual void enterOwnerStatement(MySqlParser::OwnerStatementContext *ctx) = 0;
  virtual void exitOwnerStatement(MySqlParser::OwnerStatementContext *ctx) = 0;

  virtual void enterPreciseSchedule(MySqlParser::PreciseScheduleContext *ctx) = 0;
  virtual void exitPreciseSchedule(MySqlParser::PreciseScheduleContext *ctx) = 0;

  virtual void enterIntervalSchedule(MySqlParser::IntervalScheduleContext *ctx) = 0;
  virtual void exitIntervalSchedule(MySqlParser::IntervalScheduleContext *ctx) = 0;

  virtual void enterTimestampValue(MySqlParser::TimestampValueContext *ctx) = 0;
  virtual void exitTimestampValue(MySqlParser::TimestampValueContext *ctx) = 0;

  virtual void enterIntervalExpr(MySqlParser::IntervalExprContext *ctx) = 0;
  virtual void exitIntervalExpr(MySqlParser::IntervalExprContext *ctx) = 0;

  virtual void enterIntervalType(MySqlParser::IntervalTypeContext *ctx) = 0;
  virtual void exitIntervalType(MySqlParser::IntervalTypeContext *ctx) = 0;

  virtual void enterEnableType(MySqlParser::EnableTypeContext *ctx) = 0;
  virtual void exitEnableType(MySqlParser::EnableTypeContext *ctx) = 0;

  virtual void enterIndexType(MySqlParser::IndexTypeContext *ctx) = 0;
  virtual void exitIndexType(MySqlParser::IndexTypeContext *ctx) = 0;

  virtual void enterIndexOption(MySqlParser::IndexOptionContext *ctx) = 0;
  virtual void exitIndexOption(MySqlParser::IndexOptionContext *ctx) = 0;

  virtual void enterProcedureParameter(MySqlParser::ProcedureParameterContext *ctx) = 0;
  virtual void exitProcedureParameter(MySqlParser::ProcedureParameterContext *ctx) = 0;

  virtual void enterFunctionParameter(MySqlParser::FunctionParameterContext *ctx) = 0;
  virtual void exitFunctionParameter(MySqlParser::FunctionParameterContext *ctx) = 0;

  virtual void enterRoutineComment(MySqlParser::RoutineCommentContext *ctx) = 0;
  virtual void exitRoutineComment(MySqlParser::RoutineCommentContext *ctx) = 0;

  virtual void enterRoutineLanguage(MySqlParser::RoutineLanguageContext *ctx) = 0;
  virtual void exitRoutineLanguage(MySqlParser::RoutineLanguageContext *ctx) = 0;

  virtual void enterRoutineBehavior(MySqlParser::RoutineBehaviorContext *ctx) = 0;
  virtual void exitRoutineBehavior(MySqlParser::RoutineBehaviorContext *ctx) = 0;

  virtual void enterRoutineData(MySqlParser::RoutineDataContext *ctx) = 0;
  virtual void exitRoutineData(MySqlParser::RoutineDataContext *ctx) = 0;

  virtual void enterRoutineSecurity(MySqlParser::RoutineSecurityContext *ctx) = 0;
  virtual void exitRoutineSecurity(MySqlParser::RoutineSecurityContext *ctx) = 0;

  virtual void enterServerOption(MySqlParser::ServerOptionContext *ctx) = 0;
  virtual void exitServerOption(MySqlParser::ServerOptionContext *ctx) = 0;

  virtual void enterCreateDefinitions(MySqlParser::CreateDefinitionsContext *ctx) = 0;
  virtual void exitCreateDefinitions(MySqlParser::CreateDefinitionsContext *ctx) = 0;

  virtual void enterColumnDeclaration(MySqlParser::ColumnDeclarationContext *ctx) = 0;
  virtual void exitColumnDeclaration(MySqlParser::ColumnDeclarationContext *ctx) = 0;

  virtual void enterConstraintDeclaration(MySqlParser::ConstraintDeclarationContext *ctx) = 0;
  virtual void exitConstraintDeclaration(MySqlParser::ConstraintDeclarationContext *ctx) = 0;

  virtual void enterIndexDeclaration(MySqlParser::IndexDeclarationContext *ctx) = 0;
  virtual void exitIndexDeclaration(MySqlParser::IndexDeclarationContext *ctx) = 0;

  virtual void enterColumnDefinition(MySqlParser::ColumnDefinitionContext *ctx) = 0;
  virtual void exitColumnDefinition(MySqlParser::ColumnDefinitionContext *ctx) = 0;

  virtual void enterNullColumnConstraint(MySqlParser::NullColumnConstraintContext *ctx) = 0;
  virtual void exitNullColumnConstraint(MySqlParser::NullColumnConstraintContext *ctx) = 0;

  virtual void enterDefaultColumnConstraint(MySqlParser::DefaultColumnConstraintContext *ctx) = 0;
  virtual void exitDefaultColumnConstraint(MySqlParser::DefaultColumnConstraintContext *ctx) = 0;

  virtual void enterVisibilityColumnConstraint(MySqlParser::VisibilityColumnConstraintContext *ctx) = 0;
  virtual void exitVisibilityColumnConstraint(MySqlParser::VisibilityColumnConstraintContext *ctx) = 0;

  virtual void enterInvisibilityColumnConstraint(MySqlParser::InvisibilityColumnConstraintContext *ctx) = 0;
  virtual void exitInvisibilityColumnConstraint(MySqlParser::InvisibilityColumnConstraintContext *ctx) = 0;

  virtual void enterAutoIncrementColumnConstraint(MySqlParser::AutoIncrementColumnConstraintContext *ctx) = 0;
  virtual void exitAutoIncrementColumnConstraint(MySqlParser::AutoIncrementColumnConstraintContext *ctx) = 0;

  virtual void enterPrimaryKeyColumnConstraint(MySqlParser::PrimaryKeyColumnConstraintContext *ctx) = 0;
  virtual void exitPrimaryKeyColumnConstraint(MySqlParser::PrimaryKeyColumnConstraintContext *ctx) = 0;

  virtual void enterUniqueKeyColumnConstraint(MySqlParser::UniqueKeyColumnConstraintContext *ctx) = 0;
  virtual void exitUniqueKeyColumnConstraint(MySqlParser::UniqueKeyColumnConstraintContext *ctx) = 0;

  virtual void enterCommentColumnConstraint(MySqlParser::CommentColumnConstraintContext *ctx) = 0;
  virtual void exitCommentColumnConstraint(MySqlParser::CommentColumnConstraintContext *ctx) = 0;

  virtual void enterFormatColumnConstraint(MySqlParser::FormatColumnConstraintContext *ctx) = 0;
  virtual void exitFormatColumnConstraint(MySqlParser::FormatColumnConstraintContext *ctx) = 0;

  virtual void enterStorageColumnConstraint(MySqlParser::StorageColumnConstraintContext *ctx) = 0;
  virtual void exitStorageColumnConstraint(MySqlParser::StorageColumnConstraintContext *ctx) = 0;

  virtual void enterReferenceColumnConstraint(MySqlParser::ReferenceColumnConstraintContext *ctx) = 0;
  virtual void exitReferenceColumnConstraint(MySqlParser::ReferenceColumnConstraintContext *ctx) = 0;

  virtual void enterCollateColumnConstraint(MySqlParser::CollateColumnConstraintContext *ctx) = 0;
  virtual void exitCollateColumnConstraint(MySqlParser::CollateColumnConstraintContext *ctx) = 0;

  virtual void enterGeneratedColumnConstraint(MySqlParser::GeneratedColumnConstraintContext *ctx) = 0;
  virtual void exitGeneratedColumnConstraint(MySqlParser::GeneratedColumnConstraintContext *ctx) = 0;

  virtual void enterSerialDefaultColumnConstraint(MySqlParser::SerialDefaultColumnConstraintContext *ctx) = 0;
  virtual void exitSerialDefaultColumnConstraint(MySqlParser::SerialDefaultColumnConstraintContext *ctx) = 0;

  virtual void enterCheckColumnConstraint(MySqlParser::CheckColumnConstraintContext *ctx) = 0;
  virtual void exitCheckColumnConstraint(MySqlParser::CheckColumnConstraintContext *ctx) = 0;

  virtual void enterPrimaryKeyTableConstraint(MySqlParser::PrimaryKeyTableConstraintContext *ctx) = 0;
  virtual void exitPrimaryKeyTableConstraint(MySqlParser::PrimaryKeyTableConstraintContext *ctx) = 0;

  virtual void enterUniqueKeyTableConstraint(MySqlParser::UniqueKeyTableConstraintContext *ctx) = 0;
  virtual void exitUniqueKeyTableConstraint(MySqlParser::UniqueKeyTableConstraintContext *ctx) = 0;

  virtual void enterForeignKeyTableConstraint(MySqlParser::ForeignKeyTableConstraintContext *ctx) = 0;
  virtual void exitForeignKeyTableConstraint(MySqlParser::ForeignKeyTableConstraintContext *ctx) = 0;

  virtual void enterCheckTableConstraint(MySqlParser::CheckTableConstraintContext *ctx) = 0;
  virtual void exitCheckTableConstraint(MySqlParser::CheckTableConstraintContext *ctx) = 0;

  virtual void enterReferenceDefinition(MySqlParser::ReferenceDefinitionContext *ctx) = 0;
  virtual void exitReferenceDefinition(MySqlParser::ReferenceDefinitionContext *ctx) = 0;

  virtual void enterReferenceAction(MySqlParser::ReferenceActionContext *ctx) = 0;
  virtual void exitReferenceAction(MySqlParser::ReferenceActionContext *ctx) = 0;

  virtual void enterReferenceControlType(MySqlParser::ReferenceControlTypeContext *ctx) = 0;
  virtual void exitReferenceControlType(MySqlParser::ReferenceControlTypeContext *ctx) = 0;

  virtual void enterSimpleIndexDeclaration(MySqlParser::SimpleIndexDeclarationContext *ctx) = 0;
  virtual void exitSimpleIndexDeclaration(MySqlParser::SimpleIndexDeclarationContext *ctx) = 0;

  virtual void enterSpecialIndexDeclaration(MySqlParser::SpecialIndexDeclarationContext *ctx) = 0;
  virtual void exitSpecialIndexDeclaration(MySqlParser::SpecialIndexDeclarationContext *ctx) = 0;

  virtual void enterTableOptionEngine(MySqlParser::TableOptionEngineContext *ctx) = 0;
  virtual void exitTableOptionEngine(MySqlParser::TableOptionEngineContext *ctx) = 0;

  virtual void enterTableOptionEngineAttribute(MySqlParser::TableOptionEngineAttributeContext *ctx) = 0;
  virtual void exitTableOptionEngineAttribute(MySqlParser::TableOptionEngineAttributeContext *ctx) = 0;

  virtual void enterTableOptionAutoextendSize(MySqlParser::TableOptionAutoextendSizeContext *ctx) = 0;
  virtual void exitTableOptionAutoextendSize(MySqlParser::TableOptionAutoextendSizeContext *ctx) = 0;

  virtual void enterTableOptionAutoIncrement(MySqlParser::TableOptionAutoIncrementContext *ctx) = 0;
  virtual void exitTableOptionAutoIncrement(MySqlParser::TableOptionAutoIncrementContext *ctx) = 0;

  virtual void enterTableOptionAverage(MySqlParser::TableOptionAverageContext *ctx) = 0;
  virtual void exitTableOptionAverage(MySqlParser::TableOptionAverageContext *ctx) = 0;

  virtual void enterTableOptionCharset(MySqlParser::TableOptionCharsetContext *ctx) = 0;
  virtual void exitTableOptionCharset(MySqlParser::TableOptionCharsetContext *ctx) = 0;

  virtual void enterTableOptionChecksum(MySqlParser::TableOptionChecksumContext *ctx) = 0;
  virtual void exitTableOptionChecksum(MySqlParser::TableOptionChecksumContext *ctx) = 0;

  virtual void enterTableOptionCollate(MySqlParser::TableOptionCollateContext *ctx) = 0;
  virtual void exitTableOptionCollate(MySqlParser::TableOptionCollateContext *ctx) = 0;

  virtual void enterTableOptionComment(MySqlParser::TableOptionCommentContext *ctx) = 0;
  virtual void exitTableOptionComment(MySqlParser::TableOptionCommentContext *ctx) = 0;

  virtual void enterTableOptionCompression(MySqlParser::TableOptionCompressionContext *ctx) = 0;
  virtual void exitTableOptionCompression(MySqlParser::TableOptionCompressionContext *ctx) = 0;

  virtual void enterTableOptionConnection(MySqlParser::TableOptionConnectionContext *ctx) = 0;
  virtual void exitTableOptionConnection(MySqlParser::TableOptionConnectionContext *ctx) = 0;

  virtual void enterTableOptionDataDirectory(MySqlParser::TableOptionDataDirectoryContext *ctx) = 0;
  virtual void exitTableOptionDataDirectory(MySqlParser::TableOptionDataDirectoryContext *ctx) = 0;

  virtual void enterTableOptionDelay(MySqlParser::TableOptionDelayContext *ctx) = 0;
  virtual void exitTableOptionDelay(MySqlParser::TableOptionDelayContext *ctx) = 0;

  virtual void enterTableOptionEncryption(MySqlParser::TableOptionEncryptionContext *ctx) = 0;
  virtual void exitTableOptionEncryption(MySqlParser::TableOptionEncryptionContext *ctx) = 0;

  virtual void enterTableOptionPageCompressed(MySqlParser::TableOptionPageCompressedContext *ctx) = 0;
  virtual void exitTableOptionPageCompressed(MySqlParser::TableOptionPageCompressedContext *ctx) = 0;

  virtual void enterTableOptionPageCompressionLevel(MySqlParser::TableOptionPageCompressionLevelContext *ctx) = 0;
  virtual void exitTableOptionPageCompressionLevel(MySqlParser::TableOptionPageCompressionLevelContext *ctx) = 0;

  virtual void enterTableOptionEncryptionKeyId(MySqlParser::TableOptionEncryptionKeyIdContext *ctx) = 0;
  virtual void exitTableOptionEncryptionKeyId(MySqlParser::TableOptionEncryptionKeyIdContext *ctx) = 0;

  virtual void enterTableOptionIndexDirectory(MySqlParser::TableOptionIndexDirectoryContext *ctx) = 0;
  virtual void exitTableOptionIndexDirectory(MySqlParser::TableOptionIndexDirectoryContext *ctx) = 0;

  virtual void enterTableOptionInsertMethod(MySqlParser::TableOptionInsertMethodContext *ctx) = 0;
  virtual void exitTableOptionInsertMethod(MySqlParser::TableOptionInsertMethodContext *ctx) = 0;

  virtual void enterTableOptionKeyBlockSize(MySqlParser::TableOptionKeyBlockSizeContext *ctx) = 0;
  virtual void exitTableOptionKeyBlockSize(MySqlParser::TableOptionKeyBlockSizeContext *ctx) = 0;

  virtual void enterTableOptionMaxRows(MySqlParser::TableOptionMaxRowsContext *ctx) = 0;
  virtual void exitTableOptionMaxRows(MySqlParser::TableOptionMaxRowsContext *ctx) = 0;

  virtual void enterTableOptionMinRows(MySqlParser::TableOptionMinRowsContext *ctx) = 0;
  virtual void exitTableOptionMinRows(MySqlParser::TableOptionMinRowsContext *ctx) = 0;

  virtual void enterTableOptionPackKeys(MySqlParser::TableOptionPackKeysContext *ctx) = 0;
  virtual void exitTableOptionPackKeys(MySqlParser::TableOptionPackKeysContext *ctx) = 0;

  virtual void enterTableOptionPassword(MySqlParser::TableOptionPasswordContext *ctx) = 0;
  virtual void exitTableOptionPassword(MySqlParser::TableOptionPasswordContext *ctx) = 0;

  virtual void enterTableOptionRowFormat(MySqlParser::TableOptionRowFormatContext *ctx) = 0;
  virtual void exitTableOptionRowFormat(MySqlParser::TableOptionRowFormatContext *ctx) = 0;

  virtual void enterTableOptionStartTransaction(MySqlParser::TableOptionStartTransactionContext *ctx) = 0;
  virtual void exitTableOptionStartTransaction(MySqlParser::TableOptionStartTransactionContext *ctx) = 0;

  virtual void enterTableOptionSecondaryEngineAttribute(MySqlParser::TableOptionSecondaryEngineAttributeContext *ctx) = 0;
  virtual void exitTableOptionSecondaryEngineAttribute(MySqlParser::TableOptionSecondaryEngineAttributeContext *ctx) = 0;

  virtual void enterTableOptionRecalculation(MySqlParser::TableOptionRecalculationContext *ctx) = 0;
  virtual void exitTableOptionRecalculation(MySqlParser::TableOptionRecalculationContext *ctx) = 0;

  virtual void enterTableOptionPersistent(MySqlParser::TableOptionPersistentContext *ctx) = 0;
  virtual void exitTableOptionPersistent(MySqlParser::TableOptionPersistentContext *ctx) = 0;

  virtual void enterTableOptionSamplePage(MySqlParser::TableOptionSamplePageContext *ctx) = 0;
  virtual void exitTableOptionSamplePage(MySqlParser::TableOptionSamplePageContext *ctx) = 0;

  virtual void enterTableOptionTablespace(MySqlParser::TableOptionTablespaceContext *ctx) = 0;
  virtual void exitTableOptionTablespace(MySqlParser::TableOptionTablespaceContext *ctx) = 0;

  virtual void enterTableOptionTableType(MySqlParser::TableOptionTableTypeContext *ctx) = 0;
  virtual void exitTableOptionTableType(MySqlParser::TableOptionTableTypeContext *ctx) = 0;

  virtual void enterTableOptionTransactional(MySqlParser::TableOptionTransactionalContext *ctx) = 0;
  virtual void exitTableOptionTransactional(MySqlParser::TableOptionTransactionalContext *ctx) = 0;

  virtual void enterTableOptionUnion(MySqlParser::TableOptionUnionContext *ctx) = 0;
  virtual void exitTableOptionUnion(MySqlParser::TableOptionUnionContext *ctx) = 0;

  virtual void enterTableType(MySqlParser::TableTypeContext *ctx) = 0;
  virtual void exitTableType(MySqlParser::TableTypeContext *ctx) = 0;

  virtual void enterTablespaceStorage(MySqlParser::TablespaceStorageContext *ctx) = 0;
  virtual void exitTablespaceStorage(MySqlParser::TablespaceStorageContext *ctx) = 0;

  virtual void enterPartitionDefinitions(MySqlParser::PartitionDefinitionsContext *ctx) = 0;
  virtual void exitPartitionDefinitions(MySqlParser::PartitionDefinitionsContext *ctx) = 0;

  virtual void enterPartitionFunctionHash(MySqlParser::PartitionFunctionHashContext *ctx) = 0;
  virtual void exitPartitionFunctionHash(MySqlParser::PartitionFunctionHashContext *ctx) = 0;

  virtual void enterPartitionFunctionKey(MySqlParser::PartitionFunctionKeyContext *ctx) = 0;
  virtual void exitPartitionFunctionKey(MySqlParser::PartitionFunctionKeyContext *ctx) = 0;

  virtual void enterPartitionFunctionRange(MySqlParser::PartitionFunctionRangeContext *ctx) = 0;
  virtual void exitPartitionFunctionRange(MySqlParser::PartitionFunctionRangeContext *ctx) = 0;

  virtual void enterPartitionFunctionList(MySqlParser::PartitionFunctionListContext *ctx) = 0;
  virtual void exitPartitionFunctionList(MySqlParser::PartitionFunctionListContext *ctx) = 0;

  virtual void enterSubPartitionFunctionHash(MySqlParser::SubPartitionFunctionHashContext *ctx) = 0;
  virtual void exitSubPartitionFunctionHash(MySqlParser::SubPartitionFunctionHashContext *ctx) = 0;

  virtual void enterSubPartitionFunctionKey(MySqlParser::SubPartitionFunctionKeyContext *ctx) = 0;
  virtual void exitSubPartitionFunctionKey(MySqlParser::SubPartitionFunctionKeyContext *ctx) = 0;

  virtual void enterPartitionComparison(MySqlParser::PartitionComparisonContext *ctx) = 0;
  virtual void exitPartitionComparison(MySqlParser::PartitionComparisonContext *ctx) = 0;

  virtual void enterPartitionListAtom(MySqlParser::PartitionListAtomContext *ctx) = 0;
  virtual void exitPartitionListAtom(MySqlParser::PartitionListAtomContext *ctx) = 0;

  virtual void enterPartitionListVector(MySqlParser::PartitionListVectorContext *ctx) = 0;
  virtual void exitPartitionListVector(MySqlParser::PartitionListVectorContext *ctx) = 0;

  virtual void enterPartitionSimple(MySqlParser::PartitionSimpleContext *ctx) = 0;
  virtual void exitPartitionSimple(MySqlParser::PartitionSimpleContext *ctx) = 0;

  virtual void enterPartitionDefinerAtom(MySqlParser::PartitionDefinerAtomContext *ctx) = 0;
  virtual void exitPartitionDefinerAtom(MySqlParser::PartitionDefinerAtomContext *ctx) = 0;

  virtual void enterPartitionDefinerVector(MySqlParser::PartitionDefinerVectorContext *ctx) = 0;
  virtual void exitPartitionDefinerVector(MySqlParser::PartitionDefinerVectorContext *ctx) = 0;

  virtual void enterSubpartitionDefinition(MySqlParser::SubpartitionDefinitionContext *ctx) = 0;
  virtual void exitSubpartitionDefinition(MySqlParser::SubpartitionDefinitionContext *ctx) = 0;

  virtual void enterPartitionOptionEngine(MySqlParser::PartitionOptionEngineContext *ctx) = 0;
  virtual void exitPartitionOptionEngine(MySqlParser::PartitionOptionEngineContext *ctx) = 0;

  virtual void enterPartitionOptionComment(MySqlParser::PartitionOptionCommentContext *ctx) = 0;
  virtual void exitPartitionOptionComment(MySqlParser::PartitionOptionCommentContext *ctx) = 0;

  virtual void enterPartitionOptionDataDirectory(MySqlParser::PartitionOptionDataDirectoryContext *ctx) = 0;
  virtual void exitPartitionOptionDataDirectory(MySqlParser::PartitionOptionDataDirectoryContext *ctx) = 0;

  virtual void enterPartitionOptionIndexDirectory(MySqlParser::PartitionOptionIndexDirectoryContext *ctx) = 0;
  virtual void exitPartitionOptionIndexDirectory(MySqlParser::PartitionOptionIndexDirectoryContext *ctx) = 0;

  virtual void enterPartitionOptionMaxRows(MySqlParser::PartitionOptionMaxRowsContext *ctx) = 0;
  virtual void exitPartitionOptionMaxRows(MySqlParser::PartitionOptionMaxRowsContext *ctx) = 0;

  virtual void enterPartitionOptionMinRows(MySqlParser::PartitionOptionMinRowsContext *ctx) = 0;
  virtual void exitPartitionOptionMinRows(MySqlParser::PartitionOptionMinRowsContext *ctx) = 0;

  virtual void enterPartitionOptionTablespace(MySqlParser::PartitionOptionTablespaceContext *ctx) = 0;
  virtual void exitPartitionOptionTablespace(MySqlParser::PartitionOptionTablespaceContext *ctx) = 0;

  virtual void enterPartitionOptionNodeGroup(MySqlParser::PartitionOptionNodeGroupContext *ctx) = 0;
  virtual void exitPartitionOptionNodeGroup(MySqlParser::PartitionOptionNodeGroupContext *ctx) = 0;

  virtual void enterAlterSimpleDatabase(MySqlParser::AlterSimpleDatabaseContext *ctx) = 0;
  virtual void exitAlterSimpleDatabase(MySqlParser::AlterSimpleDatabaseContext *ctx) = 0;

  virtual void enterAlterUpgradeName(MySqlParser::AlterUpgradeNameContext *ctx) = 0;
  virtual void exitAlterUpgradeName(MySqlParser::AlterUpgradeNameContext *ctx) = 0;

  virtual void enterAlterEvent(MySqlParser::AlterEventContext *ctx) = 0;
  virtual void exitAlterEvent(MySqlParser::AlterEventContext *ctx) = 0;

  virtual void enterAlterFunction(MySqlParser::AlterFunctionContext *ctx) = 0;
  virtual void exitAlterFunction(MySqlParser::AlterFunctionContext *ctx) = 0;

  virtual void enterAlterInstance(MySqlParser::AlterInstanceContext *ctx) = 0;
  virtual void exitAlterInstance(MySqlParser::AlterInstanceContext *ctx) = 0;

  virtual void enterAlterLogfileGroup(MySqlParser::AlterLogfileGroupContext *ctx) = 0;
  virtual void exitAlterLogfileGroup(MySqlParser::AlterLogfileGroupContext *ctx) = 0;

  virtual void enterAlterProcedure(MySqlParser::AlterProcedureContext *ctx) = 0;
  virtual void exitAlterProcedure(MySqlParser::AlterProcedureContext *ctx) = 0;

  virtual void enterAlterServer(MySqlParser::AlterServerContext *ctx) = 0;
  virtual void exitAlterServer(MySqlParser::AlterServerContext *ctx) = 0;

  virtual void enterAlterTable(MySqlParser::AlterTableContext *ctx) = 0;
  virtual void exitAlterTable(MySqlParser::AlterTableContext *ctx) = 0;

  virtual void enterAlterTablespace(MySqlParser::AlterTablespaceContext *ctx) = 0;
  virtual void exitAlterTablespace(MySqlParser::AlterTablespaceContext *ctx) = 0;

  virtual void enterAlterView(MySqlParser::AlterViewContext *ctx) = 0;
  virtual void exitAlterView(MySqlParser::AlterViewContext *ctx) = 0;

  virtual void enterAlterByTableOption(MySqlParser::AlterByTableOptionContext *ctx) = 0;
  virtual void exitAlterByTableOption(MySqlParser::AlterByTableOptionContext *ctx) = 0;

  virtual void enterAlterByAddColumn(MySqlParser::AlterByAddColumnContext *ctx) = 0;
  virtual void exitAlterByAddColumn(MySqlParser::AlterByAddColumnContext *ctx) = 0;

  virtual void enterAlterByAddColumns(MySqlParser::AlterByAddColumnsContext *ctx) = 0;
  virtual void exitAlterByAddColumns(MySqlParser::AlterByAddColumnsContext *ctx) = 0;

  virtual void enterAlterByAddIndex(MySqlParser::AlterByAddIndexContext *ctx) = 0;
  virtual void exitAlterByAddIndex(MySqlParser::AlterByAddIndexContext *ctx) = 0;

  virtual void enterAlterByAddPrimaryKey(MySqlParser::AlterByAddPrimaryKeyContext *ctx) = 0;
  virtual void exitAlterByAddPrimaryKey(MySqlParser::AlterByAddPrimaryKeyContext *ctx) = 0;

  virtual void enterAlterByAddUniqueKey(MySqlParser::AlterByAddUniqueKeyContext *ctx) = 0;
  virtual void exitAlterByAddUniqueKey(MySqlParser::AlterByAddUniqueKeyContext *ctx) = 0;

  virtual void enterAlterByAddSpecialIndex(MySqlParser::AlterByAddSpecialIndexContext *ctx) = 0;
  virtual void exitAlterByAddSpecialIndex(MySqlParser::AlterByAddSpecialIndexContext *ctx) = 0;

  virtual void enterAlterByAddForeignKey(MySqlParser::AlterByAddForeignKeyContext *ctx) = 0;
  virtual void exitAlterByAddForeignKey(MySqlParser::AlterByAddForeignKeyContext *ctx) = 0;

  virtual void enterAlterByAddCheckTableConstraint(MySqlParser::AlterByAddCheckTableConstraintContext *ctx) = 0;
  virtual void exitAlterByAddCheckTableConstraint(MySqlParser::AlterByAddCheckTableConstraintContext *ctx) = 0;

  virtual void enterAlterByAlterCheckTableConstraint(MySqlParser::AlterByAlterCheckTableConstraintContext *ctx) = 0;
  virtual void exitAlterByAlterCheckTableConstraint(MySqlParser::AlterByAlterCheckTableConstraintContext *ctx) = 0;

  virtual void enterAlterBySetAlgorithm(MySqlParser::AlterBySetAlgorithmContext *ctx) = 0;
  virtual void exitAlterBySetAlgorithm(MySqlParser::AlterBySetAlgorithmContext *ctx) = 0;

  virtual void enterAlterByChangeDefault(MySqlParser::AlterByChangeDefaultContext *ctx) = 0;
  virtual void exitAlterByChangeDefault(MySqlParser::AlterByChangeDefaultContext *ctx) = 0;

  virtual void enterAlterByChangeColumn(MySqlParser::AlterByChangeColumnContext *ctx) = 0;
  virtual void exitAlterByChangeColumn(MySqlParser::AlterByChangeColumnContext *ctx) = 0;

  virtual void enterAlterByRenameColumn(MySqlParser::AlterByRenameColumnContext *ctx) = 0;
  virtual void exitAlterByRenameColumn(MySqlParser::AlterByRenameColumnContext *ctx) = 0;

  virtual void enterAlterByLock(MySqlParser::AlterByLockContext *ctx) = 0;
  virtual void exitAlterByLock(MySqlParser::AlterByLockContext *ctx) = 0;

  virtual void enterAlterByModifyColumn(MySqlParser::AlterByModifyColumnContext *ctx) = 0;
  virtual void exitAlterByModifyColumn(MySqlParser::AlterByModifyColumnContext *ctx) = 0;

  virtual void enterAlterByDropColumn(MySqlParser::AlterByDropColumnContext *ctx) = 0;
  virtual void exitAlterByDropColumn(MySqlParser::AlterByDropColumnContext *ctx) = 0;

  virtual void enterAlterByDropConstraintCheck(MySqlParser::AlterByDropConstraintCheckContext *ctx) = 0;
  virtual void exitAlterByDropConstraintCheck(MySqlParser::AlterByDropConstraintCheckContext *ctx) = 0;

  virtual void enterAlterByDropPrimaryKey(MySqlParser::AlterByDropPrimaryKeyContext *ctx) = 0;
  virtual void exitAlterByDropPrimaryKey(MySqlParser::AlterByDropPrimaryKeyContext *ctx) = 0;

  virtual void enterAlterByDropIndex(MySqlParser::AlterByDropIndexContext *ctx) = 0;
  virtual void exitAlterByDropIndex(MySqlParser::AlterByDropIndexContext *ctx) = 0;

  virtual void enterAlterByRenameIndex(MySqlParser::AlterByRenameIndexContext *ctx) = 0;
  virtual void exitAlterByRenameIndex(MySqlParser::AlterByRenameIndexContext *ctx) = 0;

  virtual void enterAlterByAlterColumnDefault(MySqlParser::AlterByAlterColumnDefaultContext *ctx) = 0;
  virtual void exitAlterByAlterColumnDefault(MySqlParser::AlterByAlterColumnDefaultContext *ctx) = 0;

  virtual void enterAlterByAlterIndexVisibility(MySqlParser::AlterByAlterIndexVisibilityContext *ctx) = 0;
  virtual void exitAlterByAlterIndexVisibility(MySqlParser::AlterByAlterIndexVisibilityContext *ctx) = 0;

  virtual void enterAlterByDropForeignKey(MySqlParser::AlterByDropForeignKeyContext *ctx) = 0;
  virtual void exitAlterByDropForeignKey(MySqlParser::AlterByDropForeignKeyContext *ctx) = 0;

  virtual void enterAlterByDisableKeys(MySqlParser::AlterByDisableKeysContext *ctx) = 0;
  virtual void exitAlterByDisableKeys(MySqlParser::AlterByDisableKeysContext *ctx) = 0;

  virtual void enterAlterByEnableKeys(MySqlParser::AlterByEnableKeysContext *ctx) = 0;
  virtual void exitAlterByEnableKeys(MySqlParser::AlterByEnableKeysContext *ctx) = 0;

  virtual void enterAlterByRename(MySqlParser::AlterByRenameContext *ctx) = 0;
  virtual void exitAlterByRename(MySqlParser::AlterByRenameContext *ctx) = 0;

  virtual void enterAlterByOrder(MySqlParser::AlterByOrderContext *ctx) = 0;
  virtual void exitAlterByOrder(MySqlParser::AlterByOrderContext *ctx) = 0;

  virtual void enterAlterByConvertCharset(MySqlParser::AlterByConvertCharsetContext *ctx) = 0;
  virtual void exitAlterByConvertCharset(MySqlParser::AlterByConvertCharsetContext *ctx) = 0;

  virtual void enterAlterByDefaultCharset(MySqlParser::AlterByDefaultCharsetContext *ctx) = 0;
  virtual void exitAlterByDefaultCharset(MySqlParser::AlterByDefaultCharsetContext *ctx) = 0;

  virtual void enterAlterByDiscardTablespace(MySqlParser::AlterByDiscardTablespaceContext *ctx) = 0;
  virtual void exitAlterByDiscardTablespace(MySqlParser::AlterByDiscardTablespaceContext *ctx) = 0;

  virtual void enterAlterByImportTablespace(MySqlParser::AlterByImportTablespaceContext *ctx) = 0;
  virtual void exitAlterByImportTablespace(MySqlParser::AlterByImportTablespaceContext *ctx) = 0;

  virtual void enterAlterByForce(MySqlParser::AlterByForceContext *ctx) = 0;
  virtual void exitAlterByForce(MySqlParser::AlterByForceContext *ctx) = 0;

  virtual void enterAlterByValidate(MySqlParser::AlterByValidateContext *ctx) = 0;
  virtual void exitAlterByValidate(MySqlParser::AlterByValidateContext *ctx) = 0;

  virtual void enterAlterByAddDefinitions(MySqlParser::AlterByAddDefinitionsContext *ctx) = 0;
  virtual void exitAlterByAddDefinitions(MySqlParser::AlterByAddDefinitionsContext *ctx) = 0;

  virtual void enterAlterPartition(MySqlParser::AlterPartitionContext *ctx) = 0;
  virtual void exitAlterPartition(MySqlParser::AlterPartitionContext *ctx) = 0;

  virtual void enterAlterByAddPartition(MySqlParser::AlterByAddPartitionContext *ctx) = 0;
  virtual void exitAlterByAddPartition(MySqlParser::AlterByAddPartitionContext *ctx) = 0;

  virtual void enterAlterByDropPartition(MySqlParser::AlterByDropPartitionContext *ctx) = 0;
  virtual void exitAlterByDropPartition(MySqlParser::AlterByDropPartitionContext *ctx) = 0;

  virtual void enterAlterByDiscardPartition(MySqlParser::AlterByDiscardPartitionContext *ctx) = 0;
  virtual void exitAlterByDiscardPartition(MySqlParser::AlterByDiscardPartitionContext *ctx) = 0;

  virtual void enterAlterByImportPartition(MySqlParser::AlterByImportPartitionContext *ctx) = 0;
  virtual void exitAlterByImportPartition(MySqlParser::AlterByImportPartitionContext *ctx) = 0;

  virtual void enterAlterByTruncatePartition(MySqlParser::AlterByTruncatePartitionContext *ctx) = 0;
  virtual void exitAlterByTruncatePartition(MySqlParser::AlterByTruncatePartitionContext *ctx) = 0;

  virtual void enterAlterByCoalescePartition(MySqlParser::AlterByCoalescePartitionContext *ctx) = 0;
  virtual void exitAlterByCoalescePartition(MySqlParser::AlterByCoalescePartitionContext *ctx) = 0;

  virtual void enterAlterByReorganizePartition(MySqlParser::AlterByReorganizePartitionContext *ctx) = 0;
  virtual void exitAlterByReorganizePartition(MySqlParser::AlterByReorganizePartitionContext *ctx) = 0;

  virtual void enterAlterByExchangePartition(MySqlParser::AlterByExchangePartitionContext *ctx) = 0;
  virtual void exitAlterByExchangePartition(MySqlParser::AlterByExchangePartitionContext *ctx) = 0;

  virtual void enterAlterByAnalyzePartition(MySqlParser::AlterByAnalyzePartitionContext *ctx) = 0;
  virtual void exitAlterByAnalyzePartition(MySqlParser::AlterByAnalyzePartitionContext *ctx) = 0;

  virtual void enterAlterByCheckPartition(MySqlParser::AlterByCheckPartitionContext *ctx) = 0;
  virtual void exitAlterByCheckPartition(MySqlParser::AlterByCheckPartitionContext *ctx) = 0;

  virtual void enterAlterByOptimizePartition(MySqlParser::AlterByOptimizePartitionContext *ctx) = 0;
  virtual void exitAlterByOptimizePartition(MySqlParser::AlterByOptimizePartitionContext *ctx) = 0;

  virtual void enterAlterByRebuildPartition(MySqlParser::AlterByRebuildPartitionContext *ctx) = 0;
  virtual void exitAlterByRebuildPartition(MySqlParser::AlterByRebuildPartitionContext *ctx) = 0;

  virtual void enterAlterByRepairPartition(MySqlParser::AlterByRepairPartitionContext *ctx) = 0;
  virtual void exitAlterByRepairPartition(MySqlParser::AlterByRepairPartitionContext *ctx) = 0;

  virtual void enterAlterByRemovePartitioning(MySqlParser::AlterByRemovePartitioningContext *ctx) = 0;
  virtual void exitAlterByRemovePartitioning(MySqlParser::AlterByRemovePartitioningContext *ctx) = 0;

  virtual void enterAlterByUpgradePartitioning(MySqlParser::AlterByUpgradePartitioningContext *ctx) = 0;
  virtual void exitAlterByUpgradePartitioning(MySqlParser::AlterByUpgradePartitioningContext *ctx) = 0;

  virtual void enterDropDatabase(MySqlParser::DropDatabaseContext *ctx) = 0;
  virtual void exitDropDatabase(MySqlParser::DropDatabaseContext *ctx) = 0;

  virtual void enterDropEvent(MySqlParser::DropEventContext *ctx) = 0;
  virtual void exitDropEvent(MySqlParser::DropEventContext *ctx) = 0;

  virtual void enterDropIndex(MySqlParser::DropIndexContext *ctx) = 0;
  virtual void exitDropIndex(MySqlParser::DropIndexContext *ctx) = 0;

  virtual void enterDropLogfileGroup(MySqlParser::DropLogfileGroupContext *ctx) = 0;
  virtual void exitDropLogfileGroup(MySqlParser::DropLogfileGroupContext *ctx) = 0;

  virtual void enterDropProcedure(MySqlParser::DropProcedureContext *ctx) = 0;
  virtual void exitDropProcedure(MySqlParser::DropProcedureContext *ctx) = 0;

  virtual void enterDropFunction(MySqlParser::DropFunctionContext *ctx) = 0;
  virtual void exitDropFunction(MySqlParser::DropFunctionContext *ctx) = 0;

  virtual void enterDropServer(MySqlParser::DropServerContext *ctx) = 0;
  virtual void exitDropServer(MySqlParser::DropServerContext *ctx) = 0;

  virtual void enterDropTable(MySqlParser::DropTableContext *ctx) = 0;
  virtual void exitDropTable(MySqlParser::DropTableContext *ctx) = 0;

  virtual void enterDropTablespace(MySqlParser::DropTablespaceContext *ctx) = 0;
  virtual void exitDropTablespace(MySqlParser::DropTablespaceContext *ctx) = 0;

  virtual void enterDropTrigger(MySqlParser::DropTriggerContext *ctx) = 0;
  virtual void exitDropTrigger(MySqlParser::DropTriggerContext *ctx) = 0;

  virtual void enterDropView(MySqlParser::DropViewContext *ctx) = 0;
  virtual void exitDropView(MySqlParser::DropViewContext *ctx) = 0;

  virtual void enterDropRole(MySqlParser::DropRoleContext *ctx) = 0;
  virtual void exitDropRole(MySqlParser::DropRoleContext *ctx) = 0;

  virtual void enterSetRole(MySqlParser::SetRoleContext *ctx) = 0;
  virtual void exitSetRole(MySqlParser::SetRoleContext *ctx) = 0;

  virtual void enterRenameTable(MySqlParser::RenameTableContext *ctx) = 0;
  virtual void exitRenameTable(MySqlParser::RenameTableContext *ctx) = 0;

  virtual void enterRenameTableClause(MySqlParser::RenameTableClauseContext *ctx) = 0;
  virtual void exitRenameTableClause(MySqlParser::RenameTableClauseContext *ctx) = 0;

  virtual void enterTruncateTable(MySqlParser::TruncateTableContext *ctx) = 0;
  virtual void exitTruncateTable(MySqlParser::TruncateTableContext *ctx) = 0;

  virtual void enterCallStatement(MySqlParser::CallStatementContext *ctx) = 0;
  virtual void exitCallStatement(MySqlParser::CallStatementContext *ctx) = 0;

  virtual void enterDeleteStatement(MySqlParser::DeleteStatementContext *ctx) = 0;
  virtual void exitDeleteStatement(MySqlParser::DeleteStatementContext *ctx) = 0;

  virtual void enterDoStatement(MySqlParser::DoStatementContext *ctx) = 0;
  virtual void exitDoStatement(MySqlParser::DoStatementContext *ctx) = 0;

  virtual void enterHandlerStatement(MySqlParser::HandlerStatementContext *ctx) = 0;
  virtual void exitHandlerStatement(MySqlParser::HandlerStatementContext *ctx) = 0;

  virtual void enterInsertStatement(MySqlParser::InsertStatementContext *ctx) = 0;
  virtual void exitInsertStatement(MySqlParser::InsertStatementContext *ctx) = 0;

  virtual void enterLoadDataStatement(MySqlParser::LoadDataStatementContext *ctx) = 0;
  virtual void exitLoadDataStatement(MySqlParser::LoadDataStatementContext *ctx) = 0;

  virtual void enterLoadXmlStatement(MySqlParser::LoadXmlStatementContext *ctx) = 0;
  virtual void exitLoadXmlStatement(MySqlParser::LoadXmlStatementContext *ctx) = 0;

  virtual void enterReplaceStatement(MySqlParser::ReplaceStatementContext *ctx) = 0;
  virtual void exitReplaceStatement(MySqlParser::ReplaceStatementContext *ctx) = 0;

  virtual void enterSimpleSelect(MySqlParser::SimpleSelectContext *ctx) = 0;
  virtual void exitSimpleSelect(MySqlParser::SimpleSelectContext *ctx) = 0;

  virtual void enterParenthesisSelect(MySqlParser::ParenthesisSelectContext *ctx) = 0;
  virtual void exitParenthesisSelect(MySqlParser::ParenthesisSelectContext *ctx) = 0;

  virtual void enterUnionSelect(MySqlParser::UnionSelectContext *ctx) = 0;
  virtual void exitUnionSelect(MySqlParser::UnionSelectContext *ctx) = 0;

  virtual void enterUnionParenthesisSelect(MySqlParser::UnionParenthesisSelectContext *ctx) = 0;
  virtual void exitUnionParenthesisSelect(MySqlParser::UnionParenthesisSelectContext *ctx) = 0;

  virtual void enterWithLateralStatement(MySqlParser::WithLateralStatementContext *ctx) = 0;
  virtual void exitWithLateralStatement(MySqlParser::WithLateralStatementContext *ctx) = 0;

  virtual void enterUpdateStatement(MySqlParser::UpdateStatementContext *ctx) = 0;
  virtual void exitUpdateStatement(MySqlParser::UpdateStatementContext *ctx) = 0;

  virtual void enterValuesStatement(MySqlParser::ValuesStatementContext *ctx) = 0;
  virtual void exitValuesStatement(MySqlParser::ValuesStatementContext *ctx) = 0;

  virtual void enterInsertStatementValue(MySqlParser::InsertStatementValueContext *ctx) = 0;
  virtual void exitInsertStatementValue(MySqlParser::InsertStatementValueContext *ctx) = 0;

  virtual void enterUpdatedElement(MySqlParser::UpdatedElementContext *ctx) = 0;
  virtual void exitUpdatedElement(MySqlParser::UpdatedElementContext *ctx) = 0;

  virtual void enterAssignmentField(MySqlParser::AssignmentFieldContext *ctx) = 0;
  virtual void exitAssignmentField(MySqlParser::AssignmentFieldContext *ctx) = 0;

  virtual void enterLockClause(MySqlParser::LockClauseContext *ctx) = 0;
  virtual void exitLockClause(MySqlParser::LockClauseContext *ctx) = 0;

  virtual void enterSingleDeleteStatement(MySqlParser::SingleDeleteStatementContext *ctx) = 0;
  virtual void exitSingleDeleteStatement(MySqlParser::SingleDeleteStatementContext *ctx) = 0;

  virtual void enterMultipleDeleteStatement(MySqlParser::MultipleDeleteStatementContext *ctx) = 0;
  virtual void exitMultipleDeleteStatement(MySqlParser::MultipleDeleteStatementContext *ctx) = 0;

  virtual void enterHandlerOpenStatement(MySqlParser::HandlerOpenStatementContext *ctx) = 0;
  virtual void exitHandlerOpenStatement(MySqlParser::HandlerOpenStatementContext *ctx) = 0;

  virtual void enterHandlerReadIndexStatement(MySqlParser::HandlerReadIndexStatementContext *ctx) = 0;
  virtual void exitHandlerReadIndexStatement(MySqlParser::HandlerReadIndexStatementContext *ctx) = 0;

  virtual void enterHandlerReadStatement(MySqlParser::HandlerReadStatementContext *ctx) = 0;
  virtual void exitHandlerReadStatement(MySqlParser::HandlerReadStatementContext *ctx) = 0;

  virtual void enterHandlerCloseStatement(MySqlParser::HandlerCloseStatementContext *ctx) = 0;
  virtual void exitHandlerCloseStatement(MySqlParser::HandlerCloseStatementContext *ctx) = 0;

  virtual void enterSingleUpdateStatement(MySqlParser::SingleUpdateStatementContext *ctx) = 0;
  virtual void exitSingleUpdateStatement(MySqlParser::SingleUpdateStatementContext *ctx) = 0;

  virtual void enterMultipleUpdateStatement(MySqlParser::MultipleUpdateStatementContext *ctx) = 0;
  virtual void exitMultipleUpdateStatement(MySqlParser::MultipleUpdateStatementContext *ctx) = 0;

  virtual void enterOrderByClause(MySqlParser::OrderByClauseContext *ctx) = 0;
  virtual void exitOrderByClause(MySqlParser::OrderByClauseContext *ctx) = 0;

  virtual void enterOrderByExpression(MySqlParser::OrderByExpressionContext *ctx) = 0;
  virtual void exitOrderByExpression(MySqlParser::OrderByExpressionContext *ctx) = 0;

  virtual void enterTableSources(MySqlParser::TableSourcesContext *ctx) = 0;
  virtual void exitTableSources(MySqlParser::TableSourcesContext *ctx) = 0;

  virtual void enterTableSourceBase(MySqlParser::TableSourceBaseContext *ctx) = 0;
  virtual void exitTableSourceBase(MySqlParser::TableSourceBaseContext *ctx) = 0;

  virtual void enterTableSourceNested(MySqlParser::TableSourceNestedContext *ctx) = 0;
  virtual void exitTableSourceNested(MySqlParser::TableSourceNestedContext *ctx) = 0;

  virtual void enterTableJson(MySqlParser::TableJsonContext *ctx) = 0;
  virtual void exitTableJson(MySqlParser::TableJsonContext *ctx) = 0;

  virtual void enterAtomTableItem(MySqlParser::AtomTableItemContext *ctx) = 0;
  virtual void exitAtomTableItem(MySqlParser::AtomTableItemContext *ctx) = 0;

  virtual void enterSubqueryTableItem(MySqlParser::SubqueryTableItemContext *ctx) = 0;
  virtual void exitSubqueryTableItem(MySqlParser::SubqueryTableItemContext *ctx) = 0;

  virtual void enterTableSourcesItem(MySqlParser::TableSourcesItemContext *ctx) = 0;
  virtual void exitTableSourcesItem(MySqlParser::TableSourcesItemContext *ctx) = 0;

  virtual void enterIndexHint(MySqlParser::IndexHintContext *ctx) = 0;
  virtual void exitIndexHint(MySqlParser::IndexHintContext *ctx) = 0;

  virtual void enterIndexHintType(MySqlParser::IndexHintTypeContext *ctx) = 0;
  virtual void exitIndexHintType(MySqlParser::IndexHintTypeContext *ctx) = 0;

  virtual void enterInnerJoin(MySqlParser::InnerJoinContext *ctx) = 0;
  virtual void exitInnerJoin(MySqlParser::InnerJoinContext *ctx) = 0;

  virtual void enterStraightJoin(MySqlParser::StraightJoinContext *ctx) = 0;
  virtual void exitStraightJoin(MySqlParser::StraightJoinContext *ctx) = 0;

  virtual void enterOuterJoin(MySqlParser::OuterJoinContext *ctx) = 0;
  virtual void exitOuterJoin(MySqlParser::OuterJoinContext *ctx) = 0;

  virtual void enterNaturalJoin(MySqlParser::NaturalJoinContext *ctx) = 0;
  virtual void exitNaturalJoin(MySqlParser::NaturalJoinContext *ctx) = 0;

  virtual void enterQueryExpression(MySqlParser::QueryExpressionContext *ctx) = 0;
  virtual void exitQueryExpression(MySqlParser::QueryExpressionContext *ctx) = 0;

  virtual void enterQueryExpressionNointo(MySqlParser::QueryExpressionNointoContext *ctx) = 0;
  virtual void exitQueryExpressionNointo(MySqlParser::QueryExpressionNointoContext *ctx) = 0;

  virtual void enterQuerySpecification(MySqlParser::QuerySpecificationContext *ctx) = 0;
  virtual void exitQuerySpecification(MySqlParser::QuerySpecificationContext *ctx) = 0;

  virtual void enterQuerySpecificationNointo(MySqlParser::QuerySpecificationNointoContext *ctx) = 0;
  virtual void exitQuerySpecificationNointo(MySqlParser::QuerySpecificationNointoContext *ctx) = 0;

  virtual void enterUnionParenthesis(MySqlParser::UnionParenthesisContext *ctx) = 0;
  virtual void exitUnionParenthesis(MySqlParser::UnionParenthesisContext *ctx) = 0;

  virtual void enterUnionStatement(MySqlParser::UnionStatementContext *ctx) = 0;
  virtual void exitUnionStatement(MySqlParser::UnionStatementContext *ctx) = 0;

  virtual void enterLateralStatement(MySqlParser::LateralStatementContext *ctx) = 0;
  virtual void exitLateralStatement(MySqlParser::LateralStatementContext *ctx) = 0;

  virtual void enterJsonTable(MySqlParser::JsonTableContext *ctx) = 0;
  virtual void exitJsonTable(MySqlParser::JsonTableContext *ctx) = 0;

  virtual void enterJsonColumnList(MySqlParser::JsonColumnListContext *ctx) = 0;
  virtual void exitJsonColumnList(MySqlParser::JsonColumnListContext *ctx) = 0;

  virtual void enterJsonColumn(MySqlParser::JsonColumnContext *ctx) = 0;
  virtual void exitJsonColumn(MySqlParser::JsonColumnContext *ctx) = 0;

  virtual void enterJsonOnEmpty(MySqlParser::JsonOnEmptyContext *ctx) = 0;
  virtual void exitJsonOnEmpty(MySqlParser::JsonOnEmptyContext *ctx) = 0;

  virtual void enterJsonOnError(MySqlParser::JsonOnErrorContext *ctx) = 0;
  virtual void exitJsonOnError(MySqlParser::JsonOnErrorContext *ctx) = 0;

  virtual void enterSelectSpec(MySqlParser::SelectSpecContext *ctx) = 0;
  virtual void exitSelectSpec(MySqlParser::SelectSpecContext *ctx) = 0;

  virtual void enterSelectElements(MySqlParser::SelectElementsContext *ctx) = 0;
  virtual void exitSelectElements(MySqlParser::SelectElementsContext *ctx) = 0;

  virtual void enterSelectStarElement(MySqlParser::SelectStarElementContext *ctx) = 0;
  virtual void exitSelectStarElement(MySqlParser::SelectStarElementContext *ctx) = 0;

  virtual void enterSelectColumnElement(MySqlParser::SelectColumnElementContext *ctx) = 0;
  virtual void exitSelectColumnElement(MySqlParser::SelectColumnElementContext *ctx) = 0;

  virtual void enterSelectFunctionElement(MySqlParser::SelectFunctionElementContext *ctx) = 0;
  virtual void exitSelectFunctionElement(MySqlParser::SelectFunctionElementContext *ctx) = 0;

  virtual void enterSelectExpressionElement(MySqlParser::SelectExpressionElementContext *ctx) = 0;
  virtual void exitSelectExpressionElement(MySqlParser::SelectExpressionElementContext *ctx) = 0;

  virtual void enterSelectIntoVariables(MySqlParser::SelectIntoVariablesContext *ctx) = 0;
  virtual void exitSelectIntoVariables(MySqlParser::SelectIntoVariablesContext *ctx) = 0;

  virtual void enterSelectIntoDumpFile(MySqlParser::SelectIntoDumpFileContext *ctx) = 0;
  virtual void exitSelectIntoDumpFile(MySqlParser::SelectIntoDumpFileContext *ctx) = 0;

  virtual void enterSelectIntoTextFile(MySqlParser::SelectIntoTextFileContext *ctx) = 0;
  virtual void exitSelectIntoTextFile(MySqlParser::SelectIntoTextFileContext *ctx) = 0;

  virtual void enterSelectFieldsInto(MySqlParser::SelectFieldsIntoContext *ctx) = 0;
  virtual void exitSelectFieldsInto(MySqlParser::SelectFieldsIntoContext *ctx) = 0;

  virtual void enterSelectLinesInto(MySqlParser::SelectLinesIntoContext *ctx) = 0;
  virtual void exitSelectLinesInto(MySqlParser::SelectLinesIntoContext *ctx) = 0;

  virtual void enterFromClause(MySqlParser::FromClauseContext *ctx) = 0;
  virtual void exitFromClause(MySqlParser::FromClauseContext *ctx) = 0;

  virtual void enterGroupByClause(MySqlParser::GroupByClauseContext *ctx) = 0;
  virtual void exitGroupByClause(MySqlParser::GroupByClauseContext *ctx) = 0;

  virtual void enterHavingClause(MySqlParser::HavingClauseContext *ctx) = 0;
  virtual void exitHavingClause(MySqlParser::HavingClauseContext *ctx) = 0;

  virtual void enterWindowClause(MySqlParser::WindowClauseContext *ctx) = 0;
  virtual void exitWindowClause(MySqlParser::WindowClauseContext *ctx) = 0;

  virtual void enterGroupByItem(MySqlParser::GroupByItemContext *ctx) = 0;
  virtual void exitGroupByItem(MySqlParser::GroupByItemContext *ctx) = 0;

  virtual void enterLimitClause(MySqlParser::LimitClauseContext *ctx) = 0;
  virtual void exitLimitClause(MySqlParser::LimitClauseContext *ctx) = 0;

  virtual void enterLimitClauseAtom(MySqlParser::LimitClauseAtomContext *ctx) = 0;
  virtual void exitLimitClauseAtom(MySqlParser::LimitClauseAtomContext *ctx) = 0;

  virtual void enterStartTransaction(MySqlParser::StartTransactionContext *ctx) = 0;
  virtual void exitStartTransaction(MySqlParser::StartTransactionContext *ctx) = 0;

  virtual void enterBeginWork(MySqlParser::BeginWorkContext *ctx) = 0;
  virtual void exitBeginWork(MySqlParser::BeginWorkContext *ctx) = 0;

  virtual void enterCommitWork(MySqlParser::CommitWorkContext *ctx) = 0;
  virtual void exitCommitWork(MySqlParser::CommitWorkContext *ctx) = 0;

  virtual void enterRollbackWork(MySqlParser::RollbackWorkContext *ctx) = 0;
  virtual void exitRollbackWork(MySqlParser::RollbackWorkContext *ctx) = 0;

  virtual void enterSavepointStatement(MySqlParser::SavepointStatementContext *ctx) = 0;
  virtual void exitSavepointStatement(MySqlParser::SavepointStatementContext *ctx) = 0;

  virtual void enterRollbackStatement(MySqlParser::RollbackStatementContext *ctx) = 0;
  virtual void exitRollbackStatement(MySqlParser::RollbackStatementContext *ctx) = 0;

  virtual void enterReleaseStatement(MySqlParser::ReleaseStatementContext *ctx) = 0;
  virtual void exitReleaseStatement(MySqlParser::ReleaseStatementContext *ctx) = 0;

  virtual void enterLockTables(MySqlParser::LockTablesContext *ctx) = 0;
  virtual void exitLockTables(MySqlParser::LockTablesContext *ctx) = 0;

  virtual void enterUnlockTables(MySqlParser::UnlockTablesContext *ctx) = 0;
  virtual void exitUnlockTables(MySqlParser::UnlockTablesContext *ctx) = 0;

  virtual void enterSetAutocommitStatement(MySqlParser::SetAutocommitStatementContext *ctx) = 0;
  virtual void exitSetAutocommitStatement(MySqlParser::SetAutocommitStatementContext *ctx) = 0;

  virtual void enterSetTransactionStatement(MySqlParser::SetTransactionStatementContext *ctx) = 0;
  virtual void exitSetTransactionStatement(MySqlParser::SetTransactionStatementContext *ctx) = 0;

  virtual void enterTransactionMode(MySqlParser::TransactionModeContext *ctx) = 0;
  virtual void exitTransactionMode(MySqlParser::TransactionModeContext *ctx) = 0;

  virtual void enterLockTableElement(MySqlParser::LockTableElementContext *ctx) = 0;
  virtual void exitLockTableElement(MySqlParser::LockTableElementContext *ctx) = 0;

  virtual void enterLockAction(MySqlParser::LockActionContext *ctx) = 0;
  virtual void exitLockAction(MySqlParser::LockActionContext *ctx) = 0;

  virtual void enterTransactionOption(MySqlParser::TransactionOptionContext *ctx) = 0;
  virtual void exitTransactionOption(MySqlParser::TransactionOptionContext *ctx) = 0;

  virtual void enterTransactionLevel(MySqlParser::TransactionLevelContext *ctx) = 0;
  virtual void exitTransactionLevel(MySqlParser::TransactionLevelContext *ctx) = 0;

  virtual void enterChangeMaster(MySqlParser::ChangeMasterContext *ctx) = 0;
  virtual void exitChangeMaster(MySqlParser::ChangeMasterContext *ctx) = 0;

  virtual void enterChangeReplicationFilter(MySqlParser::ChangeReplicationFilterContext *ctx) = 0;
  virtual void exitChangeReplicationFilter(MySqlParser::ChangeReplicationFilterContext *ctx) = 0;

  virtual void enterPurgeBinaryLogs(MySqlParser::PurgeBinaryLogsContext *ctx) = 0;
  virtual void exitPurgeBinaryLogs(MySqlParser::PurgeBinaryLogsContext *ctx) = 0;

  virtual void enterResetMaster(MySqlParser::ResetMasterContext *ctx) = 0;
  virtual void exitResetMaster(MySqlParser::ResetMasterContext *ctx) = 0;

  virtual void enterResetSlave(MySqlParser::ResetSlaveContext *ctx) = 0;
  virtual void exitResetSlave(MySqlParser::ResetSlaveContext *ctx) = 0;

  virtual void enterStartSlave(MySqlParser::StartSlaveContext *ctx) = 0;
  virtual void exitStartSlave(MySqlParser::StartSlaveContext *ctx) = 0;

  virtual void enterStopSlave(MySqlParser::StopSlaveContext *ctx) = 0;
  virtual void exitStopSlave(MySqlParser::StopSlaveContext *ctx) = 0;

  virtual void enterStartGroupReplication(MySqlParser::StartGroupReplicationContext *ctx) = 0;
  virtual void exitStartGroupReplication(MySqlParser::StartGroupReplicationContext *ctx) = 0;

  virtual void enterStopGroupReplication(MySqlParser::StopGroupReplicationContext *ctx) = 0;
  virtual void exitStopGroupReplication(MySqlParser::StopGroupReplicationContext *ctx) = 0;

  virtual void enterMasterStringOption(MySqlParser::MasterStringOptionContext *ctx) = 0;
  virtual void exitMasterStringOption(MySqlParser::MasterStringOptionContext *ctx) = 0;

  virtual void enterMasterDecimalOption(MySqlParser::MasterDecimalOptionContext *ctx) = 0;
  virtual void exitMasterDecimalOption(MySqlParser::MasterDecimalOptionContext *ctx) = 0;

  virtual void enterMasterBoolOption(MySqlParser::MasterBoolOptionContext *ctx) = 0;
  virtual void exitMasterBoolOption(MySqlParser::MasterBoolOptionContext *ctx) = 0;

  virtual void enterMasterRealOption(MySqlParser::MasterRealOptionContext *ctx) = 0;
  virtual void exitMasterRealOption(MySqlParser::MasterRealOptionContext *ctx) = 0;

  virtual void enterMasterUidListOption(MySqlParser::MasterUidListOptionContext *ctx) = 0;
  virtual void exitMasterUidListOption(MySqlParser::MasterUidListOptionContext *ctx) = 0;

  virtual void enterStringMasterOption(MySqlParser::StringMasterOptionContext *ctx) = 0;
  virtual void exitStringMasterOption(MySqlParser::StringMasterOptionContext *ctx) = 0;

  virtual void enterDecimalMasterOption(MySqlParser::DecimalMasterOptionContext *ctx) = 0;
  virtual void exitDecimalMasterOption(MySqlParser::DecimalMasterOptionContext *ctx) = 0;

  virtual void enterBoolMasterOption(MySqlParser::BoolMasterOptionContext *ctx) = 0;
  virtual void exitBoolMasterOption(MySqlParser::BoolMasterOptionContext *ctx) = 0;

  virtual void enterChannelOption(MySqlParser::ChannelOptionContext *ctx) = 0;
  virtual void exitChannelOption(MySqlParser::ChannelOptionContext *ctx) = 0;

  virtual void enterDoDbReplication(MySqlParser::DoDbReplicationContext *ctx) = 0;
  virtual void exitDoDbReplication(MySqlParser::DoDbReplicationContext *ctx) = 0;

  virtual void enterIgnoreDbReplication(MySqlParser::IgnoreDbReplicationContext *ctx) = 0;
  virtual void exitIgnoreDbReplication(MySqlParser::IgnoreDbReplicationContext *ctx) = 0;

  virtual void enterDoTableReplication(MySqlParser::DoTableReplicationContext *ctx) = 0;
  virtual void exitDoTableReplication(MySqlParser::DoTableReplicationContext *ctx) = 0;

  virtual void enterIgnoreTableReplication(MySqlParser::IgnoreTableReplicationContext *ctx) = 0;
  virtual void exitIgnoreTableReplication(MySqlParser::IgnoreTableReplicationContext *ctx) = 0;

  virtual void enterWildDoTableReplication(MySqlParser::WildDoTableReplicationContext *ctx) = 0;
  virtual void exitWildDoTableReplication(MySqlParser::WildDoTableReplicationContext *ctx) = 0;

  virtual void enterWildIgnoreTableReplication(MySqlParser::WildIgnoreTableReplicationContext *ctx) = 0;
  virtual void exitWildIgnoreTableReplication(MySqlParser::WildIgnoreTableReplicationContext *ctx) = 0;

  virtual void enterRewriteDbReplication(MySqlParser::RewriteDbReplicationContext *ctx) = 0;
  virtual void exitRewriteDbReplication(MySqlParser::RewriteDbReplicationContext *ctx) = 0;

  virtual void enterTablePair(MySqlParser::TablePairContext *ctx) = 0;
  virtual void exitTablePair(MySqlParser::TablePairContext *ctx) = 0;

  virtual void enterThreadType(MySqlParser::ThreadTypeContext *ctx) = 0;
  virtual void exitThreadType(MySqlParser::ThreadTypeContext *ctx) = 0;

  virtual void enterGtidsUntilOption(MySqlParser::GtidsUntilOptionContext *ctx) = 0;
  virtual void exitGtidsUntilOption(MySqlParser::GtidsUntilOptionContext *ctx) = 0;

  virtual void enterMasterLogUntilOption(MySqlParser::MasterLogUntilOptionContext *ctx) = 0;
  virtual void exitMasterLogUntilOption(MySqlParser::MasterLogUntilOptionContext *ctx) = 0;

  virtual void enterRelayLogUntilOption(MySqlParser::RelayLogUntilOptionContext *ctx) = 0;
  virtual void exitRelayLogUntilOption(MySqlParser::RelayLogUntilOptionContext *ctx) = 0;

  virtual void enterSqlGapsUntilOption(MySqlParser::SqlGapsUntilOptionContext *ctx) = 0;
  virtual void exitSqlGapsUntilOption(MySqlParser::SqlGapsUntilOptionContext *ctx) = 0;

  virtual void enterUserConnectionOption(MySqlParser::UserConnectionOptionContext *ctx) = 0;
  virtual void exitUserConnectionOption(MySqlParser::UserConnectionOptionContext *ctx) = 0;

  virtual void enterPasswordConnectionOption(MySqlParser::PasswordConnectionOptionContext *ctx) = 0;
  virtual void exitPasswordConnectionOption(MySqlParser::PasswordConnectionOptionContext *ctx) = 0;

  virtual void enterDefaultAuthConnectionOption(MySqlParser::DefaultAuthConnectionOptionContext *ctx) = 0;
  virtual void exitDefaultAuthConnectionOption(MySqlParser::DefaultAuthConnectionOptionContext *ctx) = 0;

  virtual void enterPluginDirConnectionOption(MySqlParser::PluginDirConnectionOptionContext *ctx) = 0;
  virtual void exitPluginDirConnectionOption(MySqlParser::PluginDirConnectionOptionContext *ctx) = 0;

  virtual void enterGtuidSet(MySqlParser::GtuidSetContext *ctx) = 0;
  virtual void exitGtuidSet(MySqlParser::GtuidSetContext *ctx) = 0;

  virtual void enterXaStartTransaction(MySqlParser::XaStartTransactionContext *ctx) = 0;
  virtual void exitXaStartTransaction(MySqlParser::XaStartTransactionContext *ctx) = 0;

  virtual void enterXaEndTransaction(MySqlParser::XaEndTransactionContext *ctx) = 0;
  virtual void exitXaEndTransaction(MySqlParser::XaEndTransactionContext *ctx) = 0;

  virtual void enterXaPrepareStatement(MySqlParser::XaPrepareStatementContext *ctx) = 0;
  virtual void exitXaPrepareStatement(MySqlParser::XaPrepareStatementContext *ctx) = 0;

  virtual void enterXaCommitWork(MySqlParser::XaCommitWorkContext *ctx) = 0;
  virtual void exitXaCommitWork(MySqlParser::XaCommitWorkContext *ctx) = 0;

  virtual void enterXaRollbackWork(MySqlParser::XaRollbackWorkContext *ctx) = 0;
  virtual void exitXaRollbackWork(MySqlParser::XaRollbackWorkContext *ctx) = 0;

  virtual void enterXaRecoverWork(MySqlParser::XaRecoverWorkContext *ctx) = 0;
  virtual void exitXaRecoverWork(MySqlParser::XaRecoverWorkContext *ctx) = 0;

  virtual void enterPrepareStatement(MySqlParser::PrepareStatementContext *ctx) = 0;
  virtual void exitPrepareStatement(MySqlParser::PrepareStatementContext *ctx) = 0;

  virtual void enterExecuteStatement(MySqlParser::ExecuteStatementContext *ctx) = 0;
  virtual void exitExecuteStatement(MySqlParser::ExecuteStatementContext *ctx) = 0;

  virtual void enterDeallocatePrepare(MySqlParser::DeallocatePrepareContext *ctx) = 0;
  virtual void exitDeallocatePrepare(MySqlParser::DeallocatePrepareContext *ctx) = 0;

  virtual void enterRoutineBody(MySqlParser::RoutineBodyContext *ctx) = 0;
  virtual void exitRoutineBody(MySqlParser::RoutineBodyContext *ctx) = 0;

  virtual void enterBlockStatement(MySqlParser::BlockStatementContext *ctx) = 0;
  virtual void exitBlockStatement(MySqlParser::BlockStatementContext *ctx) = 0;

  virtual void enterCaseStatement(MySqlParser::CaseStatementContext *ctx) = 0;
  virtual void exitCaseStatement(MySqlParser::CaseStatementContext *ctx) = 0;

  virtual void enterIfStatement(MySqlParser::IfStatementContext *ctx) = 0;
  virtual void exitIfStatement(MySqlParser::IfStatementContext *ctx) = 0;

  virtual void enterIterateStatement(MySqlParser::IterateStatementContext *ctx) = 0;
  virtual void exitIterateStatement(MySqlParser::IterateStatementContext *ctx) = 0;

  virtual void enterLeaveStatement(MySqlParser::LeaveStatementContext *ctx) = 0;
  virtual void exitLeaveStatement(MySqlParser::LeaveStatementContext *ctx) = 0;

  virtual void enterLoopStatement(MySqlParser::LoopStatementContext *ctx) = 0;
  virtual void exitLoopStatement(MySqlParser::LoopStatementContext *ctx) = 0;

  virtual void enterRepeatStatement(MySqlParser::RepeatStatementContext *ctx) = 0;
  virtual void exitRepeatStatement(MySqlParser::RepeatStatementContext *ctx) = 0;

  virtual void enterReturnStatement(MySqlParser::ReturnStatementContext *ctx) = 0;
  virtual void exitReturnStatement(MySqlParser::ReturnStatementContext *ctx) = 0;

  virtual void enterWhileStatement(MySqlParser::WhileStatementContext *ctx) = 0;
  virtual void exitWhileStatement(MySqlParser::WhileStatementContext *ctx) = 0;

  virtual void enterCloseCursor(MySqlParser::CloseCursorContext *ctx) = 0;
  virtual void exitCloseCursor(MySqlParser::CloseCursorContext *ctx) = 0;

  virtual void enterFetchCursor(MySqlParser::FetchCursorContext *ctx) = 0;
  virtual void exitFetchCursor(MySqlParser::FetchCursorContext *ctx) = 0;

  virtual void enterOpenCursor(MySqlParser::OpenCursorContext *ctx) = 0;
  virtual void exitOpenCursor(MySqlParser::OpenCursorContext *ctx) = 0;

  virtual void enterDeclareVariable(MySqlParser::DeclareVariableContext *ctx) = 0;
  virtual void exitDeclareVariable(MySqlParser::DeclareVariableContext *ctx) = 0;

  virtual void enterDeclareCondition(MySqlParser::DeclareConditionContext *ctx) = 0;
  virtual void exitDeclareCondition(MySqlParser::DeclareConditionContext *ctx) = 0;

  virtual void enterDeclareCursor(MySqlParser::DeclareCursorContext *ctx) = 0;
  virtual void exitDeclareCursor(MySqlParser::DeclareCursorContext *ctx) = 0;

  virtual void enterDeclareHandler(MySqlParser::DeclareHandlerContext *ctx) = 0;
  virtual void exitDeclareHandler(MySqlParser::DeclareHandlerContext *ctx) = 0;

  virtual void enterHandlerConditionCode(MySqlParser::HandlerConditionCodeContext *ctx) = 0;
  virtual void exitHandlerConditionCode(MySqlParser::HandlerConditionCodeContext *ctx) = 0;

  virtual void enterHandlerConditionState(MySqlParser::HandlerConditionStateContext *ctx) = 0;
  virtual void exitHandlerConditionState(MySqlParser::HandlerConditionStateContext *ctx) = 0;

  virtual void enterHandlerConditionName(MySqlParser::HandlerConditionNameContext *ctx) = 0;
  virtual void exitHandlerConditionName(MySqlParser::HandlerConditionNameContext *ctx) = 0;

  virtual void enterHandlerConditionWarning(MySqlParser::HandlerConditionWarningContext *ctx) = 0;
  virtual void exitHandlerConditionWarning(MySqlParser::HandlerConditionWarningContext *ctx) = 0;

  virtual void enterHandlerConditionNotfound(MySqlParser::HandlerConditionNotfoundContext *ctx) = 0;
  virtual void exitHandlerConditionNotfound(MySqlParser::HandlerConditionNotfoundContext *ctx) = 0;

  virtual void enterHandlerConditionException(MySqlParser::HandlerConditionExceptionContext *ctx) = 0;
  virtual void exitHandlerConditionException(MySqlParser::HandlerConditionExceptionContext *ctx) = 0;

  virtual void enterProcedureSqlStatement(MySqlParser::ProcedureSqlStatementContext *ctx) = 0;
  virtual void exitProcedureSqlStatement(MySqlParser::ProcedureSqlStatementContext *ctx) = 0;

  virtual void enterCaseAlternative(MySqlParser::CaseAlternativeContext *ctx) = 0;
  virtual void exitCaseAlternative(MySqlParser::CaseAlternativeContext *ctx) = 0;

  virtual void enterElifAlternative(MySqlParser::ElifAlternativeContext *ctx) = 0;
  virtual void exitElifAlternative(MySqlParser::ElifAlternativeContext *ctx) = 0;

  virtual void enterAlterUserMysqlV56(MySqlParser::AlterUserMysqlV56Context *ctx) = 0;
  virtual void exitAlterUserMysqlV56(MySqlParser::AlterUserMysqlV56Context *ctx) = 0;

  virtual void enterAlterUserMysqlV80(MySqlParser::AlterUserMysqlV80Context *ctx) = 0;
  virtual void exitAlterUserMysqlV80(MySqlParser::AlterUserMysqlV80Context *ctx) = 0;

  virtual void enterCreateUserMysqlV56(MySqlParser::CreateUserMysqlV56Context *ctx) = 0;
  virtual void exitCreateUserMysqlV56(MySqlParser::CreateUserMysqlV56Context *ctx) = 0;

  virtual void enterCreateUserMysqlV80(MySqlParser::CreateUserMysqlV80Context *ctx) = 0;
  virtual void exitCreateUserMysqlV80(MySqlParser::CreateUserMysqlV80Context *ctx) = 0;

  virtual void enterDropUser(MySqlParser::DropUserContext *ctx) = 0;
  virtual void exitDropUser(MySqlParser::DropUserContext *ctx) = 0;

  virtual void enterGrantStatement(MySqlParser::GrantStatementContext *ctx) = 0;
  virtual void exitGrantStatement(MySqlParser::GrantStatementContext *ctx) = 0;

  virtual void enterRoleOption(MySqlParser::RoleOptionContext *ctx) = 0;
  virtual void exitRoleOption(MySqlParser::RoleOptionContext *ctx) = 0;

  virtual void enterGrantProxy(MySqlParser::GrantProxyContext *ctx) = 0;
  virtual void exitGrantProxy(MySqlParser::GrantProxyContext *ctx) = 0;

  virtual void enterRenameUser(MySqlParser::RenameUserContext *ctx) = 0;
  virtual void exitRenameUser(MySqlParser::RenameUserContext *ctx) = 0;

  virtual void enterDetailRevoke(MySqlParser::DetailRevokeContext *ctx) = 0;
  virtual void exitDetailRevoke(MySqlParser::DetailRevokeContext *ctx) = 0;

  virtual void enterShortRevoke(MySqlParser::ShortRevokeContext *ctx) = 0;
  virtual void exitShortRevoke(MySqlParser::ShortRevokeContext *ctx) = 0;

  virtual void enterRoleRevoke(MySqlParser::RoleRevokeContext *ctx) = 0;
  virtual void exitRoleRevoke(MySqlParser::RoleRevokeContext *ctx) = 0;

  virtual void enterRevokeProxy(MySqlParser::RevokeProxyContext *ctx) = 0;
  virtual void exitRevokeProxy(MySqlParser::RevokeProxyContext *ctx) = 0;

  virtual void enterSetPasswordStatement(MySqlParser::SetPasswordStatementContext *ctx) = 0;
  virtual void exitSetPasswordStatement(MySqlParser::SetPasswordStatementContext *ctx) = 0;

  virtual void enterUserSpecification(MySqlParser::UserSpecificationContext *ctx) = 0;
  virtual void exitUserSpecification(MySqlParser::UserSpecificationContext *ctx) = 0;

  virtual void enterHashAuthOption(MySqlParser::HashAuthOptionContext *ctx) = 0;
  virtual void exitHashAuthOption(MySqlParser::HashAuthOptionContext *ctx) = 0;

  virtual void enterStringAuthOption(MySqlParser::StringAuthOptionContext *ctx) = 0;
  virtual void exitStringAuthOption(MySqlParser::StringAuthOptionContext *ctx) = 0;

  virtual void enterModuleAuthOption(MySqlParser::ModuleAuthOptionContext *ctx) = 0;
  virtual void exitModuleAuthOption(MySqlParser::ModuleAuthOptionContext *ctx) = 0;

  virtual void enterSimpleAuthOption(MySqlParser::SimpleAuthOptionContext *ctx) = 0;
  virtual void exitSimpleAuthOption(MySqlParser::SimpleAuthOptionContext *ctx) = 0;

  virtual void enterModule(MySqlParser::ModuleContext *ctx) = 0;
  virtual void exitModule(MySqlParser::ModuleContext *ctx) = 0;

  virtual void enterPasswordModuleOption(MySqlParser::PasswordModuleOptionContext *ctx) = 0;
  virtual void exitPasswordModuleOption(MySqlParser::PasswordModuleOptionContext *ctx) = 0;

  virtual void enterTlsOption(MySqlParser::TlsOptionContext *ctx) = 0;
  virtual void exitTlsOption(MySqlParser::TlsOptionContext *ctx) = 0;

  virtual void enterUserResourceOption(MySqlParser::UserResourceOptionContext *ctx) = 0;
  virtual void exitUserResourceOption(MySqlParser::UserResourceOptionContext *ctx) = 0;

  virtual void enterUserPasswordOption(MySqlParser::UserPasswordOptionContext *ctx) = 0;
  virtual void exitUserPasswordOption(MySqlParser::UserPasswordOptionContext *ctx) = 0;

  virtual void enterUserLockOption(MySqlParser::UserLockOptionContext *ctx) = 0;
  virtual void exitUserLockOption(MySqlParser::UserLockOptionContext *ctx) = 0;

  virtual void enterPrivelegeClause(MySqlParser::PrivelegeClauseContext *ctx) = 0;
  virtual void exitPrivelegeClause(MySqlParser::PrivelegeClauseContext *ctx) = 0;

  virtual void enterPrivilege(MySqlParser::PrivilegeContext *ctx) = 0;
  virtual void exitPrivilege(MySqlParser::PrivilegeContext *ctx) = 0;

  virtual void enterCurrentSchemaPriviLevel(MySqlParser::CurrentSchemaPriviLevelContext *ctx) = 0;
  virtual void exitCurrentSchemaPriviLevel(MySqlParser::CurrentSchemaPriviLevelContext *ctx) = 0;

  virtual void enterGlobalPrivLevel(MySqlParser::GlobalPrivLevelContext *ctx) = 0;
  virtual void exitGlobalPrivLevel(MySqlParser::GlobalPrivLevelContext *ctx) = 0;

  virtual void enterDefiniteSchemaPrivLevel(MySqlParser::DefiniteSchemaPrivLevelContext *ctx) = 0;
  virtual void exitDefiniteSchemaPrivLevel(MySqlParser::DefiniteSchemaPrivLevelContext *ctx) = 0;

  virtual void enterDefiniteFullTablePrivLevel(MySqlParser::DefiniteFullTablePrivLevelContext *ctx) = 0;
  virtual void exitDefiniteFullTablePrivLevel(MySqlParser::DefiniteFullTablePrivLevelContext *ctx) = 0;

  virtual void enterDefiniteFullTablePrivLevel2(MySqlParser::DefiniteFullTablePrivLevel2Context *ctx) = 0;
  virtual void exitDefiniteFullTablePrivLevel2(MySqlParser::DefiniteFullTablePrivLevel2Context *ctx) = 0;

  virtual void enterDefiniteTablePrivLevel(MySqlParser::DefiniteTablePrivLevelContext *ctx) = 0;
  virtual void exitDefiniteTablePrivLevel(MySqlParser::DefiniteTablePrivLevelContext *ctx) = 0;

  virtual void enterRenameUserClause(MySqlParser::RenameUserClauseContext *ctx) = 0;
  virtual void exitRenameUserClause(MySqlParser::RenameUserClauseContext *ctx) = 0;

  virtual void enterAnalyzeTable(MySqlParser::AnalyzeTableContext *ctx) = 0;
  virtual void exitAnalyzeTable(MySqlParser::AnalyzeTableContext *ctx) = 0;

  virtual void enterCheckTable(MySqlParser::CheckTableContext *ctx) = 0;
  virtual void exitCheckTable(MySqlParser::CheckTableContext *ctx) = 0;

  virtual void enterChecksumTable(MySqlParser::ChecksumTableContext *ctx) = 0;
  virtual void exitChecksumTable(MySqlParser::ChecksumTableContext *ctx) = 0;

  virtual void enterOptimizeTable(MySqlParser::OptimizeTableContext *ctx) = 0;
  virtual void exitOptimizeTable(MySqlParser::OptimizeTableContext *ctx) = 0;

  virtual void enterRepairTable(MySqlParser::RepairTableContext *ctx) = 0;
  virtual void exitRepairTable(MySqlParser::RepairTableContext *ctx) = 0;

  virtual void enterCheckTableOption(MySqlParser::CheckTableOptionContext *ctx) = 0;
  virtual void exitCheckTableOption(MySqlParser::CheckTableOptionContext *ctx) = 0;

  virtual void enterCreateUdfunction(MySqlParser::CreateUdfunctionContext *ctx) = 0;
  virtual void exitCreateUdfunction(MySqlParser::CreateUdfunctionContext *ctx) = 0;

  virtual void enterInstallPlugin(MySqlParser::InstallPluginContext *ctx) = 0;
  virtual void exitInstallPlugin(MySqlParser::InstallPluginContext *ctx) = 0;

  virtual void enterUninstallPlugin(MySqlParser::UninstallPluginContext *ctx) = 0;
  virtual void exitUninstallPlugin(MySqlParser::UninstallPluginContext *ctx) = 0;

  virtual void enterSetVariable(MySqlParser::SetVariableContext *ctx) = 0;
  virtual void exitSetVariable(MySqlParser::SetVariableContext *ctx) = 0;

  virtual void enterSetCharset(MySqlParser::SetCharsetContext *ctx) = 0;
  virtual void exitSetCharset(MySqlParser::SetCharsetContext *ctx) = 0;

  virtual void enterSetNames(MySqlParser::SetNamesContext *ctx) = 0;
  virtual void exitSetNames(MySqlParser::SetNamesContext *ctx) = 0;

  virtual void enterSetPassword(MySqlParser::SetPasswordContext *ctx) = 0;
  virtual void exitSetPassword(MySqlParser::SetPasswordContext *ctx) = 0;

  virtual void enterSetTransaction(MySqlParser::SetTransactionContext *ctx) = 0;
  virtual void exitSetTransaction(MySqlParser::SetTransactionContext *ctx) = 0;

  virtual void enterSetAutocommit(MySqlParser::SetAutocommitContext *ctx) = 0;
  virtual void exitSetAutocommit(MySqlParser::SetAutocommitContext *ctx) = 0;

  virtual void enterSetNewValueInsideTrigger(MySqlParser::SetNewValueInsideTriggerContext *ctx) = 0;
  virtual void exitSetNewValueInsideTrigger(MySqlParser::SetNewValueInsideTriggerContext *ctx) = 0;

  virtual void enterShowMasterLogs(MySqlParser::ShowMasterLogsContext *ctx) = 0;
  virtual void exitShowMasterLogs(MySqlParser::ShowMasterLogsContext *ctx) = 0;

  virtual void enterShowLogEvents(MySqlParser::ShowLogEventsContext *ctx) = 0;
  virtual void exitShowLogEvents(MySqlParser::ShowLogEventsContext *ctx) = 0;

  virtual void enterShowObjectFilter(MySqlParser::ShowObjectFilterContext *ctx) = 0;
  virtual void exitShowObjectFilter(MySqlParser::ShowObjectFilterContext *ctx) = 0;

  virtual void enterShowColumns(MySqlParser::ShowColumnsContext *ctx) = 0;
  virtual void exitShowColumns(MySqlParser::ShowColumnsContext *ctx) = 0;

  virtual void enterShowCreateDb(MySqlParser::ShowCreateDbContext *ctx) = 0;
  virtual void exitShowCreateDb(MySqlParser::ShowCreateDbContext *ctx) = 0;

  virtual void enterShowCreateFullIdObject(MySqlParser::ShowCreateFullIdObjectContext *ctx) = 0;
  virtual void exitShowCreateFullIdObject(MySqlParser::ShowCreateFullIdObjectContext *ctx) = 0;

  virtual void enterShowCreateUser(MySqlParser::ShowCreateUserContext *ctx) = 0;
  virtual void exitShowCreateUser(MySqlParser::ShowCreateUserContext *ctx) = 0;

  virtual void enterShowEngine(MySqlParser::ShowEngineContext *ctx) = 0;
  virtual void exitShowEngine(MySqlParser::ShowEngineContext *ctx) = 0;

  virtual void enterShowGlobalInfo(MySqlParser::ShowGlobalInfoContext *ctx) = 0;
  virtual void exitShowGlobalInfo(MySqlParser::ShowGlobalInfoContext *ctx) = 0;

  virtual void enterShowErrors(MySqlParser::ShowErrorsContext *ctx) = 0;
  virtual void exitShowErrors(MySqlParser::ShowErrorsContext *ctx) = 0;

  virtual void enterShowCountErrors(MySqlParser::ShowCountErrorsContext *ctx) = 0;
  virtual void exitShowCountErrors(MySqlParser::ShowCountErrorsContext *ctx) = 0;

  virtual void enterShowSchemaFilter(MySqlParser::ShowSchemaFilterContext *ctx) = 0;
  virtual void exitShowSchemaFilter(MySqlParser::ShowSchemaFilterContext *ctx) = 0;

  virtual void enterShowRoutine(MySqlParser::ShowRoutineContext *ctx) = 0;
  virtual void exitShowRoutine(MySqlParser::ShowRoutineContext *ctx) = 0;

  virtual void enterShowGrants(MySqlParser::ShowGrantsContext *ctx) = 0;
  virtual void exitShowGrants(MySqlParser::ShowGrantsContext *ctx) = 0;

  virtual void enterShowIndexes(MySqlParser::ShowIndexesContext *ctx) = 0;
  virtual void exitShowIndexes(MySqlParser::ShowIndexesContext *ctx) = 0;

  virtual void enterShowOpenTables(MySqlParser::ShowOpenTablesContext *ctx) = 0;
  virtual void exitShowOpenTables(MySqlParser::ShowOpenTablesContext *ctx) = 0;

  virtual void enterShowProfile(MySqlParser::ShowProfileContext *ctx) = 0;
  virtual void exitShowProfile(MySqlParser::ShowProfileContext *ctx) = 0;

  virtual void enterShowSlaveStatus(MySqlParser::ShowSlaveStatusContext *ctx) = 0;
  virtual void exitShowSlaveStatus(MySqlParser::ShowSlaveStatusContext *ctx) = 0;

  virtual void enterVariableClause(MySqlParser::VariableClauseContext *ctx) = 0;
  virtual void exitVariableClause(MySqlParser::VariableClauseContext *ctx) = 0;

  virtual void enterShowCommonEntity(MySqlParser::ShowCommonEntityContext *ctx) = 0;
  virtual void exitShowCommonEntity(MySqlParser::ShowCommonEntityContext *ctx) = 0;

  virtual void enterShowFilter(MySqlParser::ShowFilterContext *ctx) = 0;
  virtual void exitShowFilter(MySqlParser::ShowFilterContext *ctx) = 0;

  virtual void enterShowGlobalInfoClause(MySqlParser::ShowGlobalInfoClauseContext *ctx) = 0;
  virtual void exitShowGlobalInfoClause(MySqlParser::ShowGlobalInfoClauseContext *ctx) = 0;

  virtual void enterShowSchemaEntity(MySqlParser::ShowSchemaEntityContext *ctx) = 0;
  virtual void exitShowSchemaEntity(MySqlParser::ShowSchemaEntityContext *ctx) = 0;

  virtual void enterShowProfileType(MySqlParser::ShowProfileTypeContext *ctx) = 0;
  virtual void exitShowProfileType(MySqlParser::ShowProfileTypeContext *ctx) = 0;

  virtual void enterBinlogStatement(MySqlParser::BinlogStatementContext *ctx) = 0;
  virtual void exitBinlogStatement(MySqlParser::BinlogStatementContext *ctx) = 0;

  virtual void enterCacheIndexStatement(MySqlParser::CacheIndexStatementContext *ctx) = 0;
  virtual void exitCacheIndexStatement(MySqlParser::CacheIndexStatementContext *ctx) = 0;

  virtual void enterFlushStatement(MySqlParser::FlushStatementContext *ctx) = 0;
  virtual void exitFlushStatement(MySqlParser::FlushStatementContext *ctx) = 0;

  virtual void enterKillStatement(MySqlParser::KillStatementContext *ctx) = 0;
  virtual void exitKillStatement(MySqlParser::KillStatementContext *ctx) = 0;

  virtual void enterLoadIndexIntoCache(MySqlParser::LoadIndexIntoCacheContext *ctx) = 0;
  virtual void exitLoadIndexIntoCache(MySqlParser::LoadIndexIntoCacheContext *ctx) = 0;

  virtual void enterResetStatement(MySqlParser::ResetStatementContext *ctx) = 0;
  virtual void exitResetStatement(MySqlParser::ResetStatementContext *ctx) = 0;

  virtual void enterShutdownStatement(MySqlParser::ShutdownStatementContext *ctx) = 0;
  virtual void exitShutdownStatement(MySqlParser::ShutdownStatementContext *ctx) = 0;

  virtual void enterTableIndexes(MySqlParser::TableIndexesContext *ctx) = 0;
  virtual void exitTableIndexes(MySqlParser::TableIndexesContext *ctx) = 0;

  virtual void enterSimpleFlushOption(MySqlParser::SimpleFlushOptionContext *ctx) = 0;
  virtual void exitSimpleFlushOption(MySqlParser::SimpleFlushOptionContext *ctx) = 0;

  virtual void enterChannelFlushOption(MySqlParser::ChannelFlushOptionContext *ctx) = 0;
  virtual void exitChannelFlushOption(MySqlParser::ChannelFlushOptionContext *ctx) = 0;

  virtual void enterTableFlushOption(MySqlParser::TableFlushOptionContext *ctx) = 0;
  virtual void exitTableFlushOption(MySqlParser::TableFlushOptionContext *ctx) = 0;

  virtual void enterFlushTableOption(MySqlParser::FlushTableOptionContext *ctx) = 0;
  virtual void exitFlushTableOption(MySqlParser::FlushTableOptionContext *ctx) = 0;

  virtual void enterLoadedTableIndexes(MySqlParser::LoadedTableIndexesContext *ctx) = 0;
  virtual void exitLoadedTableIndexes(MySqlParser::LoadedTableIndexesContext *ctx) = 0;

  virtual void enterSimpleDescribeStatement(MySqlParser::SimpleDescribeStatementContext *ctx) = 0;
  virtual void exitSimpleDescribeStatement(MySqlParser::SimpleDescribeStatementContext *ctx) = 0;

  virtual void enterFullDescribeStatement(MySqlParser::FullDescribeStatementContext *ctx) = 0;
  virtual void exitFullDescribeStatement(MySqlParser::FullDescribeStatementContext *ctx) = 0;

  virtual void enterHelpStatement(MySqlParser::HelpStatementContext *ctx) = 0;
  virtual void exitHelpStatement(MySqlParser::HelpStatementContext *ctx) = 0;

  virtual void enterUseStatement(MySqlParser::UseStatementContext *ctx) = 0;
  virtual void exitUseStatement(MySqlParser::UseStatementContext *ctx) = 0;

  virtual void enterSignalStatement(MySqlParser::SignalStatementContext *ctx) = 0;
  virtual void exitSignalStatement(MySqlParser::SignalStatementContext *ctx) = 0;

  virtual void enterResignalStatement(MySqlParser::ResignalStatementContext *ctx) = 0;
  virtual void exitResignalStatement(MySqlParser::ResignalStatementContext *ctx) = 0;

  virtual void enterSignalConditionInformation(MySqlParser::SignalConditionInformationContext *ctx) = 0;
  virtual void exitSignalConditionInformation(MySqlParser::SignalConditionInformationContext *ctx) = 0;

  virtual void enterWithStatement(MySqlParser::WithStatementContext *ctx) = 0;
  virtual void exitWithStatement(MySqlParser::WithStatementContext *ctx) = 0;

  virtual void enterTableStatement(MySqlParser::TableStatementContext *ctx) = 0;
  virtual void exitTableStatement(MySqlParser::TableStatementContext *ctx) = 0;

  virtual void enterDiagnosticsStatement(MySqlParser::DiagnosticsStatementContext *ctx) = 0;
  virtual void exitDiagnosticsStatement(MySqlParser::DiagnosticsStatementContext *ctx) = 0;

  virtual void enterDiagnosticsConditionInformationName(MySqlParser::DiagnosticsConditionInformationNameContext *ctx) = 0;
  virtual void exitDiagnosticsConditionInformationName(MySqlParser::DiagnosticsConditionInformationNameContext *ctx) = 0;

  virtual void enterDescribeStatements(MySqlParser::DescribeStatementsContext *ctx) = 0;
  virtual void exitDescribeStatements(MySqlParser::DescribeStatementsContext *ctx) = 0;

  virtual void enterDescribeConnection(MySqlParser::DescribeConnectionContext *ctx) = 0;
  virtual void exitDescribeConnection(MySqlParser::DescribeConnectionContext *ctx) = 0;

  virtual void enterFullId(MySqlParser::FullIdContext *ctx) = 0;
  virtual void exitFullId(MySqlParser::FullIdContext *ctx) = 0;

  virtual void enterTableName(MySqlParser::TableNameContext *ctx) = 0;
  virtual void exitTableName(MySqlParser::TableNameContext *ctx) = 0;

  virtual void enterRoleName(MySqlParser::RoleNameContext *ctx) = 0;
  virtual void exitRoleName(MySqlParser::RoleNameContext *ctx) = 0;

  virtual void enterFullColumnName(MySqlParser::FullColumnNameContext *ctx) = 0;
  virtual void exitFullColumnName(MySqlParser::FullColumnNameContext *ctx) = 0;

  virtual void enterIndexColumnName(MySqlParser::IndexColumnNameContext *ctx) = 0;
  virtual void exitIndexColumnName(MySqlParser::IndexColumnNameContext *ctx) = 0;

  virtual void enterUserName(MySqlParser::UserNameContext *ctx) = 0;
  virtual void exitUserName(MySqlParser::UserNameContext *ctx) = 0;

  virtual void enterMysqlVariable(MySqlParser::MysqlVariableContext *ctx) = 0;
  virtual void exitMysqlVariable(MySqlParser::MysqlVariableContext *ctx) = 0;

  virtual void enterCharsetName(MySqlParser::CharsetNameContext *ctx) = 0;
  virtual void exitCharsetName(MySqlParser::CharsetNameContext *ctx) = 0;

  virtual void enterCollationName(MySqlParser::CollationNameContext *ctx) = 0;
  virtual void exitCollationName(MySqlParser::CollationNameContext *ctx) = 0;

  virtual void enterEngineName(MySqlParser::EngineNameContext *ctx) = 0;
  virtual void exitEngineName(MySqlParser::EngineNameContext *ctx) = 0;

  virtual void enterEngineNameBase(MySqlParser::EngineNameBaseContext *ctx) = 0;
  virtual void exitEngineNameBase(MySqlParser::EngineNameBaseContext *ctx) = 0;

  virtual void enterUuidSet(MySqlParser::UuidSetContext *ctx) = 0;
  virtual void exitUuidSet(MySqlParser::UuidSetContext *ctx) = 0;

  virtual void enterXid(MySqlParser::XidContext *ctx) = 0;
  virtual void exitXid(MySqlParser::XidContext *ctx) = 0;

  virtual void enterXuidStringId(MySqlParser::XuidStringIdContext *ctx) = 0;
  virtual void exitXuidStringId(MySqlParser::XuidStringIdContext *ctx) = 0;

  virtual void enterAuthPlugin(MySqlParser::AuthPluginContext *ctx) = 0;
  virtual void exitAuthPlugin(MySqlParser::AuthPluginContext *ctx) = 0;

  virtual void enterUid(MySqlParser::UidContext *ctx) = 0;
  virtual void exitUid(MySqlParser::UidContext *ctx) = 0;

  virtual void enterSimpleId(MySqlParser::SimpleIdContext *ctx) = 0;
  virtual void exitSimpleId(MySqlParser::SimpleIdContext *ctx) = 0;

  virtual void enterDottedId(MySqlParser::DottedIdContext *ctx) = 0;
  virtual void exitDottedId(MySqlParser::DottedIdContext *ctx) = 0;

  virtual void enterDecimalLiteral(MySqlParser::DecimalLiteralContext *ctx) = 0;
  virtual void exitDecimalLiteral(MySqlParser::DecimalLiteralContext *ctx) = 0;

  virtual void enterFileSizeLiteral(MySqlParser::FileSizeLiteralContext *ctx) = 0;
  virtual void exitFileSizeLiteral(MySqlParser::FileSizeLiteralContext *ctx) = 0;

  virtual void enterStringLiteral(MySqlParser::StringLiteralContext *ctx) = 0;
  virtual void exitStringLiteral(MySqlParser::StringLiteralContext *ctx) = 0;

  virtual void enterBooleanLiteral(MySqlParser::BooleanLiteralContext *ctx) = 0;
  virtual void exitBooleanLiteral(MySqlParser::BooleanLiteralContext *ctx) = 0;

  virtual void enterHexadecimalLiteral(MySqlParser::HexadecimalLiteralContext *ctx) = 0;
  virtual void exitHexadecimalLiteral(MySqlParser::HexadecimalLiteralContext *ctx) = 0;

  virtual void enterNullNotnull(MySqlParser::NullNotnullContext *ctx) = 0;
  virtual void exitNullNotnull(MySqlParser::NullNotnullContext *ctx) = 0;

  virtual void enterConstant(MySqlParser::ConstantContext *ctx) = 0;
  virtual void exitConstant(MySqlParser::ConstantContext *ctx) = 0;

  virtual void enterStringDataType(MySqlParser::StringDataTypeContext *ctx) = 0;
  virtual void exitStringDataType(MySqlParser::StringDataTypeContext *ctx) = 0;

  virtual void enterNationalVaryingStringDataType(MySqlParser::NationalVaryingStringDataTypeContext *ctx) = 0;
  virtual void exitNationalVaryingStringDataType(MySqlParser::NationalVaryingStringDataTypeContext *ctx) = 0;

  virtual void enterNationalStringDataType(MySqlParser::NationalStringDataTypeContext *ctx) = 0;
  virtual void exitNationalStringDataType(MySqlParser::NationalStringDataTypeContext *ctx) = 0;

  virtual void enterDimensionDataType(MySqlParser::DimensionDataTypeContext *ctx) = 0;
  virtual void exitDimensionDataType(MySqlParser::DimensionDataTypeContext *ctx) = 0;

  virtual void enterSimpleDataType(MySqlParser::SimpleDataTypeContext *ctx) = 0;
  virtual void exitSimpleDataType(MySqlParser::SimpleDataTypeContext *ctx) = 0;

  virtual void enterCollectionDataType(MySqlParser::CollectionDataTypeContext *ctx) = 0;
  virtual void exitCollectionDataType(MySqlParser::CollectionDataTypeContext *ctx) = 0;

  virtual void enterSpatialDataType(MySqlParser::SpatialDataTypeContext *ctx) = 0;
  virtual void exitSpatialDataType(MySqlParser::SpatialDataTypeContext *ctx) = 0;

  virtual void enterLongVarcharDataType(MySqlParser::LongVarcharDataTypeContext *ctx) = 0;
  virtual void exitLongVarcharDataType(MySqlParser::LongVarcharDataTypeContext *ctx) = 0;

  virtual void enterLongVarbinaryDataType(MySqlParser::LongVarbinaryDataTypeContext *ctx) = 0;
  virtual void exitLongVarbinaryDataType(MySqlParser::LongVarbinaryDataTypeContext *ctx) = 0;

  virtual void enterCollectionOptions(MySqlParser::CollectionOptionsContext *ctx) = 0;
  virtual void exitCollectionOptions(MySqlParser::CollectionOptionsContext *ctx) = 0;

  virtual void enterConvertedDataType(MySqlParser::ConvertedDataTypeContext *ctx) = 0;
  virtual void exitConvertedDataType(MySqlParser::ConvertedDataTypeContext *ctx) = 0;

  virtual void enterLengthOneDimension(MySqlParser::LengthOneDimensionContext *ctx) = 0;
  virtual void exitLengthOneDimension(MySqlParser::LengthOneDimensionContext *ctx) = 0;

  virtual void enterLengthTwoDimension(MySqlParser::LengthTwoDimensionContext *ctx) = 0;
  virtual void exitLengthTwoDimension(MySqlParser::LengthTwoDimensionContext *ctx) = 0;

  virtual void enterLengthTwoOptionalDimension(MySqlParser::LengthTwoOptionalDimensionContext *ctx) = 0;
  virtual void exitLengthTwoOptionalDimension(MySqlParser::LengthTwoOptionalDimensionContext *ctx) = 0;

  virtual void enterUidList(MySqlParser::UidListContext *ctx) = 0;
  virtual void exitUidList(MySqlParser::UidListContext *ctx) = 0;

  virtual void enterFullColumnNameList(MySqlParser::FullColumnNameListContext *ctx) = 0;
  virtual void exitFullColumnNameList(MySqlParser::FullColumnNameListContext *ctx) = 0;

  virtual void enterTables(MySqlParser::TablesContext *ctx) = 0;
  virtual void exitTables(MySqlParser::TablesContext *ctx) = 0;

  virtual void enterIndexColumnNames(MySqlParser::IndexColumnNamesContext *ctx) = 0;
  virtual void exitIndexColumnNames(MySqlParser::IndexColumnNamesContext *ctx) = 0;

  virtual void enterExpressions(MySqlParser::ExpressionsContext *ctx) = 0;
  virtual void exitExpressions(MySqlParser::ExpressionsContext *ctx) = 0;

  virtual void enterExpressionsWithDefaults(MySqlParser::ExpressionsWithDefaultsContext *ctx) = 0;
  virtual void exitExpressionsWithDefaults(MySqlParser::ExpressionsWithDefaultsContext *ctx) = 0;

  virtual void enterConstants(MySqlParser::ConstantsContext *ctx) = 0;
  virtual void exitConstants(MySqlParser::ConstantsContext *ctx) = 0;

  virtual void enterSimpleStrings(MySqlParser::SimpleStringsContext *ctx) = 0;
  virtual void exitSimpleStrings(MySqlParser::SimpleStringsContext *ctx) = 0;

  virtual void enterUserVariables(MySqlParser::UserVariablesContext *ctx) = 0;
  virtual void exitUserVariables(MySqlParser::UserVariablesContext *ctx) = 0;

  virtual void enterDefaultValue(MySqlParser::DefaultValueContext *ctx) = 0;
  virtual void exitDefaultValue(MySqlParser::DefaultValueContext *ctx) = 0;

  virtual void enterCurrentTimestamp(MySqlParser::CurrentTimestampContext *ctx) = 0;
  virtual void exitCurrentTimestamp(MySqlParser::CurrentTimestampContext *ctx) = 0;

  virtual void enterExpressionOrDefault(MySqlParser::ExpressionOrDefaultContext *ctx) = 0;
  virtual void exitExpressionOrDefault(MySqlParser::ExpressionOrDefaultContext *ctx) = 0;

  virtual void enterIfExists(MySqlParser::IfExistsContext *ctx) = 0;
  virtual void exitIfExists(MySqlParser::IfExistsContext *ctx) = 0;

  virtual void enterIfNotExists(MySqlParser::IfNotExistsContext *ctx) = 0;
  virtual void exitIfNotExists(MySqlParser::IfNotExistsContext *ctx) = 0;

  virtual void enterOrReplace(MySqlParser::OrReplaceContext *ctx) = 0;
  virtual void exitOrReplace(MySqlParser::OrReplaceContext *ctx) = 0;

  virtual void enterWaitNowaitClause(MySqlParser::WaitNowaitClauseContext *ctx) = 0;
  virtual void exitWaitNowaitClause(MySqlParser::WaitNowaitClauseContext *ctx) = 0;

  virtual void enterSpecificFunctionCall(MySqlParser::SpecificFunctionCallContext *ctx) = 0;
  virtual void exitSpecificFunctionCall(MySqlParser::SpecificFunctionCallContext *ctx) = 0;

  virtual void enterAggregateFunctionCall(MySqlParser::AggregateFunctionCallContext *ctx) = 0;
  virtual void exitAggregateFunctionCall(MySqlParser::AggregateFunctionCallContext *ctx) = 0;

  virtual void enterNonAggregateFunctionCall(MySqlParser::NonAggregateFunctionCallContext *ctx) = 0;
  virtual void exitNonAggregateFunctionCall(MySqlParser::NonAggregateFunctionCallContext *ctx) = 0;

  virtual void enterScalarFunctionCall(MySqlParser::ScalarFunctionCallContext *ctx) = 0;
  virtual void exitScalarFunctionCall(MySqlParser::ScalarFunctionCallContext *ctx) = 0;

  virtual void enterUdfFunctionCall(MySqlParser::UdfFunctionCallContext *ctx) = 0;
  virtual void exitUdfFunctionCall(MySqlParser::UdfFunctionCallContext *ctx) = 0;

  virtual void enterPasswordFunctionCall(MySqlParser::PasswordFunctionCallContext *ctx) = 0;
  virtual void exitPasswordFunctionCall(MySqlParser::PasswordFunctionCallContext *ctx) = 0;

  virtual void enterSimpleFunctionCall(MySqlParser::SimpleFunctionCallContext *ctx) = 0;
  virtual void exitSimpleFunctionCall(MySqlParser::SimpleFunctionCallContext *ctx) = 0;

  virtual void enterDataTypeFunctionCall(MySqlParser::DataTypeFunctionCallContext *ctx) = 0;
  virtual void exitDataTypeFunctionCall(MySqlParser::DataTypeFunctionCallContext *ctx) = 0;

  virtual void enterValuesFunctionCall(MySqlParser::ValuesFunctionCallContext *ctx) = 0;
  virtual void exitValuesFunctionCall(MySqlParser::ValuesFunctionCallContext *ctx) = 0;

  virtual void enterCaseExpressionFunctionCall(MySqlParser::CaseExpressionFunctionCallContext *ctx) = 0;
  virtual void exitCaseExpressionFunctionCall(MySqlParser::CaseExpressionFunctionCallContext *ctx) = 0;

  virtual void enterCaseFunctionCall(MySqlParser::CaseFunctionCallContext *ctx) = 0;
  virtual void exitCaseFunctionCall(MySqlParser::CaseFunctionCallContext *ctx) = 0;

  virtual void enterCharFunctionCall(MySqlParser::CharFunctionCallContext *ctx) = 0;
  virtual void exitCharFunctionCall(MySqlParser::CharFunctionCallContext *ctx) = 0;

  virtual void enterPositionFunctionCall(MySqlParser::PositionFunctionCallContext *ctx) = 0;
  virtual void exitPositionFunctionCall(MySqlParser::PositionFunctionCallContext *ctx) = 0;

  virtual void enterSubstrFunctionCall(MySqlParser::SubstrFunctionCallContext *ctx) = 0;
  virtual void exitSubstrFunctionCall(MySqlParser::SubstrFunctionCallContext *ctx) = 0;

  virtual void enterTrimFunctionCall(MySqlParser::TrimFunctionCallContext *ctx) = 0;
  virtual void exitTrimFunctionCall(MySqlParser::TrimFunctionCallContext *ctx) = 0;

  virtual void enterWeightFunctionCall(MySqlParser::WeightFunctionCallContext *ctx) = 0;
  virtual void exitWeightFunctionCall(MySqlParser::WeightFunctionCallContext *ctx) = 0;

  virtual void enterExtractFunctionCall(MySqlParser::ExtractFunctionCallContext *ctx) = 0;
  virtual void exitExtractFunctionCall(MySqlParser::ExtractFunctionCallContext *ctx) = 0;

  virtual void enterGetFormatFunctionCall(MySqlParser::GetFormatFunctionCallContext *ctx) = 0;
  virtual void exitGetFormatFunctionCall(MySqlParser::GetFormatFunctionCallContext *ctx) = 0;

  virtual void enterJsonValueFunctionCall(MySqlParser::JsonValueFunctionCallContext *ctx) = 0;
  virtual void exitJsonValueFunctionCall(MySqlParser::JsonValueFunctionCallContext *ctx) = 0;

  virtual void enterCaseFuncAlternative(MySqlParser::CaseFuncAlternativeContext *ctx) = 0;
  virtual void exitCaseFuncAlternative(MySqlParser::CaseFuncAlternativeContext *ctx) = 0;

  virtual void enterLevelWeightList(MySqlParser::LevelWeightListContext *ctx) = 0;
  virtual void exitLevelWeightList(MySqlParser::LevelWeightListContext *ctx) = 0;

  virtual void enterLevelWeightRange(MySqlParser::LevelWeightRangeContext *ctx) = 0;
  virtual void exitLevelWeightRange(MySqlParser::LevelWeightRangeContext *ctx) = 0;

  virtual void enterLevelInWeightListElement(MySqlParser::LevelInWeightListElementContext *ctx) = 0;
  virtual void exitLevelInWeightListElement(MySqlParser::LevelInWeightListElementContext *ctx) = 0;

  virtual void enterAggregateWindowedFunction(MySqlParser::AggregateWindowedFunctionContext *ctx) = 0;
  virtual void exitAggregateWindowedFunction(MySqlParser::AggregateWindowedFunctionContext *ctx) = 0;

  virtual void enterNonAggregateWindowedFunction(MySqlParser::NonAggregateWindowedFunctionContext *ctx) = 0;
  virtual void exitNonAggregateWindowedFunction(MySqlParser::NonAggregateWindowedFunctionContext *ctx) = 0;

  virtual void enterOverClause(MySqlParser::OverClauseContext *ctx) = 0;
  virtual void exitOverClause(MySqlParser::OverClauseContext *ctx) = 0;

  virtual void enterWindowSpec(MySqlParser::WindowSpecContext *ctx) = 0;
  virtual void exitWindowSpec(MySqlParser::WindowSpecContext *ctx) = 0;

  virtual void enterWindowName(MySqlParser::WindowNameContext *ctx) = 0;
  virtual void exitWindowName(MySqlParser::WindowNameContext *ctx) = 0;

  virtual void enterFrameClause(MySqlParser::FrameClauseContext *ctx) = 0;
  virtual void exitFrameClause(MySqlParser::FrameClauseContext *ctx) = 0;

  virtual void enterFrameUnits(MySqlParser::FrameUnitsContext *ctx) = 0;
  virtual void exitFrameUnits(MySqlParser::FrameUnitsContext *ctx) = 0;

  virtual void enterFrameExtent(MySqlParser::FrameExtentContext *ctx) = 0;
  virtual void exitFrameExtent(MySqlParser::FrameExtentContext *ctx) = 0;

  virtual void enterFrameBetween(MySqlParser::FrameBetweenContext *ctx) = 0;
  virtual void exitFrameBetween(MySqlParser::FrameBetweenContext *ctx) = 0;

  virtual void enterFrameRange(MySqlParser::FrameRangeContext *ctx) = 0;
  virtual void exitFrameRange(MySqlParser::FrameRangeContext *ctx) = 0;

  virtual void enterPartitionClause(MySqlParser::PartitionClauseContext *ctx) = 0;
  virtual void exitPartitionClause(MySqlParser::PartitionClauseContext *ctx) = 0;

  virtual void enterScalarFunctionName(MySqlParser::ScalarFunctionNameContext *ctx) = 0;
  virtual void exitScalarFunctionName(MySqlParser::ScalarFunctionNameContext *ctx) = 0;

  virtual void enterPasswordFunctionClause(MySqlParser::PasswordFunctionClauseContext *ctx) = 0;
  virtual void exitPasswordFunctionClause(MySqlParser::PasswordFunctionClauseContext *ctx) = 0;

  virtual void enterFunctionArgs(MySqlParser::FunctionArgsContext *ctx) = 0;
  virtual void exitFunctionArgs(MySqlParser::FunctionArgsContext *ctx) = 0;

  virtual void enterFunctionArg(MySqlParser::FunctionArgContext *ctx) = 0;
  virtual void exitFunctionArg(MySqlParser::FunctionArgContext *ctx) = 0;

  virtual void enterIsExpression(MySqlParser::IsExpressionContext *ctx) = 0;
  virtual void exitIsExpression(MySqlParser::IsExpressionContext *ctx) = 0;

  virtual void enterNotExpression(MySqlParser::NotExpressionContext *ctx) = 0;
  virtual void exitNotExpression(MySqlParser::NotExpressionContext *ctx) = 0;

  virtual void enterLogicalExpression(MySqlParser::LogicalExpressionContext *ctx) = 0;
  virtual void exitLogicalExpression(MySqlParser::LogicalExpressionContext *ctx) = 0;

  virtual void enterPredicateExpression(MySqlParser::PredicateExpressionContext *ctx) = 0;
  virtual void exitPredicateExpression(MySqlParser::PredicateExpressionContext *ctx) = 0;

  virtual void enterSoundsLikePredicate(MySqlParser::SoundsLikePredicateContext *ctx) = 0;
  virtual void exitSoundsLikePredicate(MySqlParser::SoundsLikePredicateContext *ctx) = 0;

  virtual void enterExpressionAtomPredicate(MySqlParser::ExpressionAtomPredicateContext *ctx) = 0;
  virtual void exitExpressionAtomPredicate(MySqlParser::ExpressionAtomPredicateContext *ctx) = 0;

  virtual void enterSubqueryComparisonPredicate(MySqlParser::SubqueryComparisonPredicateContext *ctx) = 0;
  virtual void exitSubqueryComparisonPredicate(MySqlParser::SubqueryComparisonPredicateContext *ctx) = 0;

  virtual void enterJsonMemberOfPredicate(MySqlParser::JsonMemberOfPredicateContext *ctx) = 0;
  virtual void exitJsonMemberOfPredicate(MySqlParser::JsonMemberOfPredicateContext *ctx) = 0;

  virtual void enterBinaryComparisonPredicate(MySqlParser::BinaryComparisonPredicateContext *ctx) = 0;
  virtual void exitBinaryComparisonPredicate(MySqlParser::BinaryComparisonPredicateContext *ctx) = 0;

  virtual void enterInPredicate(MySqlParser::InPredicateContext *ctx) = 0;
  virtual void exitInPredicate(MySqlParser::InPredicateContext *ctx) = 0;

  virtual void enterBetweenPredicate(MySqlParser::BetweenPredicateContext *ctx) = 0;
  virtual void exitBetweenPredicate(MySqlParser::BetweenPredicateContext *ctx) = 0;

  virtual void enterIsNullPredicate(MySqlParser::IsNullPredicateContext *ctx) = 0;
  virtual void exitIsNullPredicate(MySqlParser::IsNullPredicateContext *ctx) = 0;

  virtual void enterLikePredicate(MySqlParser::LikePredicateContext *ctx) = 0;
  virtual void exitLikePredicate(MySqlParser::LikePredicateContext *ctx) = 0;

  virtual void enterRegexpPredicate(MySqlParser::RegexpPredicateContext *ctx) = 0;
  virtual void exitRegexpPredicate(MySqlParser::RegexpPredicateContext *ctx) = 0;

  virtual void enterUnaryExpressionAtom(MySqlParser::UnaryExpressionAtomContext *ctx) = 0;
  virtual void exitUnaryExpressionAtom(MySqlParser::UnaryExpressionAtomContext *ctx) = 0;

  virtual void enterCollateExpressionAtom(MySqlParser::CollateExpressionAtomContext *ctx) = 0;
  virtual void exitCollateExpressionAtom(MySqlParser::CollateExpressionAtomContext *ctx) = 0;

  virtual void enterMysqlVariableExpressionAtom(MySqlParser::MysqlVariableExpressionAtomContext *ctx) = 0;
  virtual void exitMysqlVariableExpressionAtom(MySqlParser::MysqlVariableExpressionAtomContext *ctx) = 0;

  virtual void enterNestedExpressionAtom(MySqlParser::NestedExpressionAtomContext *ctx) = 0;
  virtual void exitNestedExpressionAtom(MySqlParser::NestedExpressionAtomContext *ctx) = 0;

  virtual void enterNestedRowExpressionAtom(MySqlParser::NestedRowExpressionAtomContext *ctx) = 0;
  virtual void exitNestedRowExpressionAtom(MySqlParser::NestedRowExpressionAtomContext *ctx) = 0;

  virtual void enterMathExpressionAtom(MySqlParser::MathExpressionAtomContext *ctx) = 0;
  virtual void exitMathExpressionAtom(MySqlParser::MathExpressionAtomContext *ctx) = 0;

  virtual void enterExistsExpressionAtom(MySqlParser::ExistsExpressionAtomContext *ctx) = 0;
  virtual void exitExistsExpressionAtom(MySqlParser::ExistsExpressionAtomContext *ctx) = 0;

  virtual void enterIntervalExpressionAtom(MySqlParser::IntervalExpressionAtomContext *ctx) = 0;
  virtual void exitIntervalExpressionAtom(MySqlParser::IntervalExpressionAtomContext *ctx) = 0;

  virtual void enterJsonExpressionAtom(MySqlParser::JsonExpressionAtomContext *ctx) = 0;
  virtual void exitJsonExpressionAtom(MySqlParser::JsonExpressionAtomContext *ctx) = 0;

  virtual void enterSubqueryExpressionAtom(MySqlParser::SubqueryExpressionAtomContext *ctx) = 0;
  virtual void exitSubqueryExpressionAtom(MySqlParser::SubqueryExpressionAtomContext *ctx) = 0;

  virtual void enterConstantExpressionAtom(MySqlParser::ConstantExpressionAtomContext *ctx) = 0;
  virtual void exitConstantExpressionAtom(MySqlParser::ConstantExpressionAtomContext *ctx) = 0;

  virtual void enterFunctionCallExpressionAtom(MySqlParser::FunctionCallExpressionAtomContext *ctx) = 0;
  virtual void exitFunctionCallExpressionAtom(MySqlParser::FunctionCallExpressionAtomContext *ctx) = 0;

  virtual void enterBinaryExpressionAtom(MySqlParser::BinaryExpressionAtomContext *ctx) = 0;
  virtual void exitBinaryExpressionAtom(MySqlParser::BinaryExpressionAtomContext *ctx) = 0;

  virtual void enterFullColumnNameExpressionAtom(MySqlParser::FullColumnNameExpressionAtomContext *ctx) = 0;
  virtual void exitFullColumnNameExpressionAtom(MySqlParser::FullColumnNameExpressionAtomContext *ctx) = 0;

  virtual void enterBitExpressionAtom(MySqlParser::BitExpressionAtomContext *ctx) = 0;
  virtual void exitBitExpressionAtom(MySqlParser::BitExpressionAtomContext *ctx) = 0;

  virtual void enterUnaryOperator(MySqlParser::UnaryOperatorContext *ctx) = 0;
  virtual void exitUnaryOperator(MySqlParser::UnaryOperatorContext *ctx) = 0;

  virtual void enterComparisonOperator(MySqlParser::ComparisonOperatorContext *ctx) = 0;
  virtual void exitComparisonOperator(MySqlParser::ComparisonOperatorContext *ctx) = 0;

  virtual void enterLogicalOperator(MySqlParser::LogicalOperatorContext *ctx) = 0;
  virtual void exitLogicalOperator(MySqlParser::LogicalOperatorContext *ctx) = 0;

  virtual void enterBitOperator(MySqlParser::BitOperatorContext *ctx) = 0;
  virtual void exitBitOperator(MySqlParser::BitOperatorContext *ctx) = 0;

  virtual void enterMathOperator(MySqlParser::MathOperatorContext *ctx) = 0;
  virtual void exitMathOperator(MySqlParser::MathOperatorContext *ctx) = 0;

  virtual void enterJsonOperator(MySqlParser::JsonOperatorContext *ctx) = 0;
  virtual void exitJsonOperator(MySqlParser::JsonOperatorContext *ctx) = 0;

  virtual void enterCharsetNameBase(MySqlParser::CharsetNameBaseContext *ctx) = 0;
  virtual void exitCharsetNameBase(MySqlParser::CharsetNameBaseContext *ctx) = 0;

  virtual void enterTransactionLevelBase(MySqlParser::TransactionLevelBaseContext *ctx) = 0;
  virtual void exitTransactionLevelBase(MySqlParser::TransactionLevelBaseContext *ctx) = 0;

  virtual void enterPrivilegesBase(MySqlParser::PrivilegesBaseContext *ctx) = 0;
  virtual void exitPrivilegesBase(MySqlParser::PrivilegesBaseContext *ctx) = 0;

  virtual void enterIntervalTypeBase(MySqlParser::IntervalTypeBaseContext *ctx) = 0;
  virtual void exitIntervalTypeBase(MySqlParser::IntervalTypeBaseContext *ctx) = 0;

  virtual void enterDataTypeBase(MySqlParser::DataTypeBaseContext *ctx) = 0;
  virtual void exitDataTypeBase(MySqlParser::DataTypeBaseContext *ctx) = 0;

  virtual void enterKeywordsCanBeId(MySqlParser::KeywordsCanBeIdContext *ctx) = 0;
  virtual void exitKeywordsCanBeId(MySqlParser::KeywordsCanBeIdContext *ctx) = 0;

  virtual void enterFunctionNameBase(MySqlParser::FunctionNameBaseContext *ctx) = 0;
  virtual void exitFunctionNameBase(MySqlParser::FunctionNameBaseContext *ctx) = 0;


};

