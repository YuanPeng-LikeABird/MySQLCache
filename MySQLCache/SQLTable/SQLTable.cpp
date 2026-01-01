#include "SQLTable.h"
#include "Consts.h"
#include "MathUtils.h"
#include "Common.h"
#include "SQLTableContainer.h"
#include "MemoryManager.h"
#include <algorithm>
#include <unordered_set>

using namespace std;

SQLNormalTable::SQLNormalTable(SQLTableSchema *schema) :
	SQLTable(schema)
{
}

SQLNormalTable::~SQLNormalTable()
{
	FOR_EACH(i, m_pkHash) {
		delete i->second;
	}

	if (m_schema->kind() == TableKind::tkExtend) {
		delete m_schema;
	}
}

// 返回游离记录
SQLRecord *SQLNormalTable::newRecord()
{
	SQLRecord *rec = new SQLNormalRecord(this);
	return rec;
}

SQLRecord *SQLNormalTable::append(SQLRecord *rec)
{
	int64_t pk = intPK(rec);
	auto i = m_pkHash.find(pk);
	if (i != m_pkHash.end()) {
		return i->second;
	}

	m_pkHash[pk] = rec;
	return rec;
}

int SQLNormalTable::recordCount() const
{
	return m_pkHash.size();
}

void SQLNormalTable::forEach(const ForEachRecordEvent &e)
{
	FOR_EACH(i, m_pkHash) {
		e(i->second);
	}
}

FieldSchema *SQLNormalTable::primaryKey() const
{
	return normalSchema()->primaryKey();
}

SQLNormalTableSchema *SQLNormalTable::normalSchema() const
{
	return static_cast<SQLNormalTableSchema *>(m_schema);
}

std::string SQLNormalTable::name() const
{
	return m_schema->name();
}

void SQLNormalTable::update(SQLRecord *rec, std::vector<std::string> &updateFieldNames)
{
	auto n = m_pkHash.find(intPK(rec));
	if (n != m_pkHash.end()) {
		SQLRecord *dstRec = n->second;
		FOR_EACH(i, updateFieldNames) {
			FieldSchema *field = normalSchema()->findField(*i);
			if (field) {
				dstRec->setValue(*i, rec->value(*i));
			}
		}
	}
}

SQLRecord *SQLNormalTable::insert(SQLRecord *rec)
{
	SQLRecord *dstRec = newRecord();
	for (int i = 0; i < normalSchema()->fieldCount(); ++i) {
		auto &fieldName = normalSchema()->field(i)->name();
		dstRec->setValue(fieldName, rec->value(fieldName));
	}
	append(dstRec);

	return dstRec;
}

bool SQLNormalTable::remove(SQLRecord *rec)
{
	return removeByPK(intPK(rec));
}

bool SQLNormalTable::removeByPK(int64_t pk)
{
	auto i = m_pkHash.find(pk);
	if (i != m_pkHash.end()) {
		delete i->second;
		m_pkHash.erase(pk);
		return true;
	}

	return false;
}

bool SQLNormalTable::exist(SQLRecord *rec)
{
	return existPK(intPK(rec));
}

bool SQLNormalTable::existPK(int64_t pk)
{
	return m_pkHash.find(pk) != m_pkHash.end();
}

int64_t SQLNormalTable::intPK(const SQLRecord *rec) const
{
	FieldSchema *pkField = primaryKey();
	if (!pkField) {
		return 0;
	}

	return rec->value(pkField->name()).toInt64();
}

SQLRecord *SQLNormalTable::selectByPK(int64_t pk)
{
	auto i = m_pkHash.find(pk);
	if (i != m_pkHash.end()) {
		return i->second;
	}
	return nullptr;
}

void SQLNormalTable::doSave(WriteBuffer *buffer)
{
	SQLTable::doSave(buffer);
	buffer->writeInt(m_pkHash.size());
	if (m_schema->orderFieldCount() > 0) {
		std::vector<SQLRecord *> orderRecs;
		FOR_EACH(i, m_pkHash) {
			orderRecs.push_back(i->second);
		}

		std::sort(orderRecs.begin(), orderRecs.end(), [&](SQLRecord *rec1, SQLRecord *rec2) {
			return compareRecordByOrderFields(rec1, rec2);
			});
		for (int i = 0; i < orderRecs.size(); ++i) {
			orderRecs.at(i)->save(buffer);
		}
	}
	else {
		FOR_EACH(i, m_pkHash) {
			i->second->save(buffer);
		}
	}
}

void SQLNormalTable::doUnload(OutputStream &out)
{
	SQLTable::doUnload(out);
	out.writeInt(m_pkHash.size());
	FOR_EACH(i, m_pkHash) {
		out.writeLong(i->first);
		out.writeUInt(static_cast<SQLNormalRecord *>(i->second)->dataId());
	}
}

void SQLNormalTable::doLoad(InputStream &in)
{
	SQLTable::doLoad(in);
	int32_t count = in.readInt();
	for (int i = 0; i < count; ++i) {
		int64_t pk = in.readLong();
		uint32_t dataId = in.readUInt();
		m_pkHash[pk] = new SQLNormalRecord(this, dataId);
	}
}

SQLRecord::SQLRecord(SQLTable *table) :
	m_table(table)
{
}

SQLTable *SQLRecord::table() const
{
	return m_table;
}

SQLJoinTable::SQLJoinTable(SQLTableSchema *tableSchema) :
	SQLTable(tableSchema)
{
	m_leftTable.setSchema(joinSchema()->left());
	m_rightTable.setSchema(joinSchema()->right());
}

SQLJoinTable::~SQLJoinTable()
{
	FOR_EACH(i, m_leftJoinHash) {
		FOR_EACH(j, (i->second)) {
			delete *j;
		}
	}
}

SQLRecord *SQLJoinTable::newRecord()
{
	return new SQLJoinRecord(this);
}

SQLRecord *SQLJoinTable::append(SQLRecord *rec)
{
	addJoin(static_cast<SQLJoinRecord *>(rec));
	return rec;
}

int SQLJoinTable::recordCount() const
{
	int count = 0;
	FOR_EACH(i, m_leftJoinHash) {
		count += i->second.size();
	}
	return count;
}

void SQLJoinTable::forEach(const ForEachRecordEvent &e)
{
	FOR_EACH(i, m_leftJoinHash) {	
		FOR_EACH(j, (i->second)) {
			e(*j);
		}
	}
}

void SQLJoinTable::setThreadIndex(int8_t index)
{
	SQLTable::setThreadIndex(index);
	m_leftTable.setThreadIndex(index);
	m_rightTable.setThreadIndex(index);
}

SQLNormalTable &SQLJoinTable::left()
{
	return m_leftTable;
}

SQLNormalTable &SQLJoinTable::right()
{
	return m_rightTable;
}

SQLJoinTableSchema *SQLJoinTable::joinSchema() const
{
	return static_cast<SQLJoinTableSchema *>(schema());
}

SQLNormalTable *SQLJoinTable::getTable(const std::string &tableName)
{
	if (m_leftTable.schema()->name() == tableName) {
		return &m_leftTable;
	}
	else if (m_rightTable.schema()->name() == tableName) {
		return &m_rightTable;
	}
	
	return nullptr;
}

void SQLJoinTable::addJoin(SQLJoinRecord *rec)
{
	int64_t left = rec->left()->pk();
	int64_t right = rec->right()->pk();

	auto i = m_leftJoinHash.find(left);
	if (i == m_leftJoinHash.end()) {
		m_leftJoinHash[left] = JOINPKSet();
	}

	m_leftJoinHash[left].insert(rec);

	i = m_rightJoinHash.find(right);
	if (i == m_rightJoinHash.end()) {
		m_rightJoinHash[right] = JOINPKSet();
	}

	m_rightJoinHash[right].insert(rec);
}

void SQLJoinTable::removeJoin(int64_t pk, bool isLeft)
{
	JOINPKHash *src = isLeft ? &m_leftJoinHash : &m_rightJoinHash;
	JOINPKHash *dst = isLeft ? &m_rightJoinHash : &m_leftJoinHash;
	JOINPKSet &dstPKs = (*src)[pk];
	FOR_EACH(i, dstPKs) {
		SQLJoinRecord *joinRec = *i;
		JOINPKSet &srcPKs = (*dst)[isLeft? joinRec->right()->pk() : joinRec->left()->pk()];
		srcPKs.erase(joinRec);
		delete joinRec;
	}

	src->erase(pk);
}

bool SQLJoinTable::update(const std::string &tableName, SQLRecord *rec, std::vector<std::string> &updateFields)
{
	SQLNormalTable *table = getTable(tableName);
	if (!table) {
		return false;
	}

	table->update(rec, updateFields);
	return true;
}

bool SQLJoinTable::remove(const std::string &tableName, SQLRecord *rec)
{
	SQLNormalTable *table = getTable(tableName);
	if (!table) {
		return false;
	}

	removeJoin(table->intPK(rec), table == &m_leftTable);
	return table->remove(rec);
}

void SQLJoinTable::doSave(WriteBuffer* buffer)
{
	SQLTable::doSave(buffer);

	buffer->writeInt(recordCount());
	if (m_schema->orderFieldCount() > 0) {
		std::vector<SQLJoinRecord *> orderRecs;
		forEach([&](SQLRecord *rec) {
			orderRecs.push_back(static_cast<SQLJoinRecord *>(rec));
			});

		std::sort(orderRecs.begin(), orderRecs.end(), [&](SQLJoinRecord *rec1,
			SQLJoinRecord *rec2) {
				return compareRecordByOrderFields(rec1, rec2);
			});

		for (int i = 0; i < orderRecs.size(); ++i) {
			orderRecs[i]->save(buffer);
			delete orderRecs[i];
		}
	}
	else {
		forEach([&](SQLRecord *rec) {
			rec->save(buffer);
		});
	}
}

void SQLJoinTable::doUnload(OutputStream &out)
{
	SQLTable::doUnload(out);

	m_leftTable.doUnload(out);
	m_rightTable.doUnload(out);
	
	out.writeInt(m_leftJoinHash.size());
	FOR_EACH(i, m_leftJoinHash) {
		out.writeLong(i->first);
		out.writeInt(i->second.size());
		FOR_EACH(j, (i->second)) {
			out.writeLong((*j)->right()->pk());
		}
	}
}

void SQLJoinTable::doLoad(InputStream &in)
{
	SQLTable::doLoad(in);

	m_leftTable.doLoad(in);
	m_rightTable.doLoad(in);

	int cnt = in.readInt();
	while (cnt-- > 0) {
		int64_t leftPk = in.readLong();
		SQLRecord *leftRec = m_leftTable.selectByPK(leftPk);
		m_leftJoinHash[leftPk] = JOINPKSet();
		auto &left = m_leftJoinHash[leftPk];
		int rightSize = in.readInt();
		while (rightSize-- > 0) {
			int64_t rightPk = in.readLong();
			SQLRecord *rightRec = m_rightTable.selectByPK(rightPk);
			SQLJoinRecord *joinRec = new SQLJoinRecord(this, leftRec, rightRec);
			left.insert(joinRec);

			auto right = m_rightJoinHash.find(rightPk);
			if (right == m_rightJoinHash.end()) {
				m_rightJoinHash[rightPk] = JOINPKSet();
				right = m_rightJoinHash.find(rightPk);
			}

			right->second.insert(joinRec);
		}
	}
}

SQLJoinRecord::SQLJoinRecord(SQLTable *table, SQLRecord *left, SQLRecord *right) :
	SQLRecord(table),
	m_left(left),
	m_right(right)
{
}

SQLJoinRecord::~SQLJoinRecord()
{
}

void SQLJoinRecord::read(std::unordered_map<std::string, MyVariant>& sqlResult,
	bool directColumnName)
{
	SQLJoinTable *joinTable = static_cast<SQLJoinTable *>(m_table);
	SQLJoinTableSchema *joinTableSchema = static_cast<SQLJoinTableSchema *>(joinTable->schema());

	SQLNormalTableSchema *tableSchema = joinTableSchema->left();
	SQLNormalTable *table = joinTable->getTable(tableSchema->name());
	// 根据主键先看table中是否已经存在
	FieldSchema *pk = tableSchema->primaryKey();
	std::string pkFieldName = tableSchema->getRealColumnName(pk->name());
	int64_t leftPKValue = sqlResult[pkFieldName].toInt64();
	m_left = table->selectByPK(leftPKValue);
	if (!m_left) {
		m_left = table->newRecord();
		m_left->read(sqlResult);
		table->append(m_left);
	}

	tableSchema = joinTableSchema->right();
	table = joinTable->getTable(tableSchema->name());
	// 根据主键先看table中是否已经存在
	pk = tableSchema->primaryKey();
	pkFieldName = tableSchema->getRealColumnName(pk->name());
	int64_t rightPKValue = sqlResult[pkFieldName].toInt64();
	m_right = table->selectByPK(rightPKValue);
	if (!m_right) {
		m_right = table->newRecord();
		m_right->read(sqlResult);
		table->append(m_right);
	}
}

const MyVariant SQLJoinRecord::value(const std::string &fieldName) const
{
	if (m_left == nullptr || m_right == nullptr) {
		return MyVariant();
	}

	if (static_cast<SQLNormalTableSchema *>(m_left->table()->schema())->findField(fieldName)) {
		return m_left->value(fieldName);
	}
	else {
		return m_right->value(fieldName);
	}
}

std::string SQLJoinRecord::strValue(const std::string &fieldName)
{
	if (m_left == nullptr || m_right == nullptr) {
		return "";
	}

	if (static_cast<SQLNormalTableSchema *>(m_left->table()->schema())->findField(fieldName)) {
		return m_left->strValue(fieldName);
	}
	else {
		return m_right->strValue(fieldName);
	}
}

void SQLJoinRecord::setValue(const std::string &fieldName, const MyVariant &value)
{
	if (m_left == nullptr || m_right == nullptr) {
		return;
	}

	if (static_cast<SQLNormalTableSchema *>(m_left->table()->schema())->findField(fieldName)) {
		m_left->setValue(fieldName, value);
	}
	else {
		m_right->setValue(fieldName, value);
	}
}

int64_t SQLJoinRecord::pk() const
{
	return -1;
}

void SQLJoinRecord::save(WriteBuffer* buffer)
{
	m_left->save(buffer);
	m_right->save(buffer);
}

SQLRecord *SQLJoinRecord::left() const
{
	return m_left;
}

SQLRecord *SQLJoinRecord::right() const
{
	return m_right;
}

SQLJoinTable *SQLJoinRecord::joinTable() const
{
	return static_cast<SQLJoinTable *>(m_table);
}

void SQLJoinRecord::add(SQLRecord *rec)
{
	const std::string tableName = static_cast<SQLNormalTable *>(rec->table())->name();
	if (tableName == joinTable()->left().name()) {
		m_left = rec;
	}
	else if (tableName == joinTable()->right().name()) {
		m_right = rec;
	}
}

SQLTable::SQLTable(SQLTableSchema *tableSchema) :
	m_schema(tableSchema),
	m_used(0),
	m_threadIndex(-1)
{
}

SQLTable::~SQLTable()
{
}

void SQLTable::save(WriteBuffer* buffer)
{
	doSave(buffer);
	uint32_t oldUsed = m_used;
	m_used = buffer->byteLength();
	SQLTableContainer::instance(m_threadIndex)->addMemoryUsed(m_used - oldUsed);
}

ByteArray SQLTable::unload()
{
	OutputStream out;
	doUnload(out);
	return out.toByteArray();
}

void SQLTable::unload(OutputStream &out)
{
	doUnload(out);
}

void SQLTable::doSave(WriteBuffer *buffer)
{
	m_schema->save(buffer);
}

void SQLTable::doUnload(OutputStream &out)
{	
	out.writeByte((int8_t)kind());
	out.writeULong(reinterpret_cast<uint64_t>(m_schema));
	out.writeInt(m_params.count());
	for (int i = 0; i < m_params.count(); ++i) {
		out.writeVariant(m_params.variant(i));
	}
}

void SQLTable::load(ByteArray bytes)
{
	InputStream in(bytes);
	doLoad(in);
}

void SQLTable::load(InputStream &in)
{
	doLoad(in);
}

void SQLTable::doLoad(InputStream &in)
{
	m_schema = reinterpret_cast<SQLTableSchema *>(in.readULong());
	int count = in.readInt();
	for (int i = 0; i < count; ++i) {
		m_params.add(in.readVariant());
	}
}

SQLTableSchema *SQLTable::schema() const
{
	return m_schema;
}

void SQLTable::setSchema(SQLTableSchema *schema)
{
	m_schema = schema;
}

TableKind SQLTable::kind() const
{
	return m_schema->kind();
}

bool SQLTable::compareRecordByOrderFields(SQLRecord *rec1, SQLRecord *rec2)
{
	for (int i = 0; i < m_schema->orderFieldCount(); ++i) {
		const OrderFieldInfo &orderInfo = m_schema->orderField(i);
		FieldSchema *field = orderInfo.field;
		OrderType order = orderInfo.order;
		MyVariant v1 = rec1->value(field->name());
		MyVariant v2 = rec2->value(field->name());
		COMPARE_RESULT(v1, v2, order);
	}

	return false;
}

void SQLTable::setThreadIndex(int8_t index)
{
	m_threadIndex = index;
}

int8_t SQLTable::threadIndex() const
{
	return m_threadIndex;
}

MyVariants &SQLTable::params()
{
	return m_params;
}

uint32_t SQLTable::meomoryUsed() const
{
	return m_used;
}

SQLNormalRecord::SQLNormalRecord(SQLTable *table) :
	SQLRecord(table)
{
	m_dataId = 
		MemoryManager::instantce(table->threadIndex()).arrayMemory().allocate(
			m_table->schema()->recordLength());
}

SQLNormalRecord::SQLNormalRecord(SQLTable *table, uint32_t dataId) :
	SQLRecord(table),
	m_dataId(dataId)
{
}

SQLNormalRecord::~SQLNormalRecord()
{
	MemoryManager &mem = MemoryManager::instantce(m_table->threadIndex());
	ArrayMemoryManager &arrayMemory = mem.arrayMemory();
	SQLNormalTableSchema *tableSchema = static_cast<SQLNormalTableSchema *>(m_table->schema());
	for (int i = 0; i < tableSchema->fieldCount(); ++i) {
		FieldSchema *fd = tableSchema->field(i);
		if (fd->dataType() == DataType::dtString ||
			fd->dataType() == DataType::dtBlob) {
			uint32_t varId = arrayMemory.memoryOperator(m_dataId).getUint32(tableSchema->dataOffSet(fd->name()));
			mem.varMemory().clear(varId);
		}
	}
	arrayMemory.memoryOperator(m_dataId);
	arrayMemory.recycle(m_dataId);
}

void SQLNormalRecord::setValue(const std::string &fieldName, const MyVariant &value)
{
	SQLNormalTableSchema *tableSchema = static_cast<SQLNormalTableSchema *>(m_table->schema());
	int32_t offset = tableSchema->dataOffSet(fieldName);
	if (offset == -1) {
		return;
	}

	int index = tableSchema->fieldIndex(fieldName);
	auto &mm = MemoryManager::instantce(m_table->threadIndex());
	auto &memOpr = mm.arrayMemory().memoryOperator(m_dataId);
	if (value.isNull()) {
		setNull(index, true);
		return;
	}
	
	setNull(index, false);
	FieldSchema* fd = tableSchema->field(index);
	switch (fd->dataType())
	{
		case DataType::dtBoolean:
			memOpr.setInt8(offset, value.toBool() ? 1 : 0);
			break;
		case DataType::dtSmallInt:
			memOpr.setInt16(offset, value.toShort());
			break;
		case DataType::dtInt:
			memOpr.setInt32(offset, value.toInt());
			break;
		case DataType::dtBigInt:
			memOpr.setInt64(offset, value.toInt64());
			break;
		case DataType::dtFloat:
		case DataType::dtDouble:
			memOpr.setFloat64(offset, value.toDouble());
			break;
		case DataType::dtString:
		{
			std::string strValue = value.toString();
			if (strValue.empty()) {
				memOpr.setUint32(offset, 0);
			}
			else {
				uint32_t varId = mm.varMemory().set(VarData((uint8_t *)strValue.c_str(), strValue.size()));
				memOpr.setUint32(offset, varId);
			}
			break;
		}
		case DataType::dtBlob:
		{
			ByteArray blobValue = value.toBlob();
			if (blobValue->byteLength() == 0) {
				memOpr.setUint32(offset, 0);
			}
			else {
				uint32_t varId = mm.varMemory().set(VarData(blobValue->data(), blobValue->byteLength()));
				memOpr.setUint32(offset, varId);
			}
			break;
		}
		default:
			break;
	}
}

int64_t SQLNormalRecord::pk() const
{
	return static_cast<SQLNormalTable *>(m_table)->intPK(this);
}

const MyVariant SQLNormalRecord::value(const std::string &fieldName) const
{
	SQLNormalTableSchema *tableSchema = static_cast<SQLNormalTableSchema *>(m_table->schema());
	int32_t offset = tableSchema->dataOffSet(fieldName);
	int fieldIndex = tableSchema->fieldIndex(fieldName);
	if (fieldIndex < 0) {
		return MyVariant();
	}

	auto &mm = MemoryManager::instantce(m_table->threadIndex());
	auto &memOpr = mm.arrayMemory().memoryOperator(m_dataId);
	if (isNull(fieldIndex)) {
		return MyVariant();
	}

	FieldSchema* fd = tableSchema->field(fieldIndex);
	switch (fd->dataType())
	{
		case DataType::dtBoolean:
			return memOpr.getInt8(offset) == 1 ? true : false;
		case DataType::dtSmallInt:
			return memOpr.getInt16(offset);
		case DataType::dtInt:
			return memOpr.getInt32(offset);
		case DataType::dtBigInt:
			return memOpr.getInt64(offset);
		case DataType::dtFloat:
		case DataType::dtDouble:
			return memOpr.getFloat64(offset);
		case DataType::dtString:
		{
			uint32_t varId = memOpr.getUint32(offset);
			if (varId == 0) {
				return string();
			}

			VarData vData = mm.varMemory().get(varId);
			return string((char *)vData.data, vData.len);
		}
		case DataType::dtBlob:
		{
			uint32_t varId = memOpr.getUint32(offset);
			if (varId == 0) {
				return ByteArray();
			}

			VarData vData = mm.varMemory().get(varId);
			return ByteArray::from(vData.data, vData.len);
		}
		default:
			break;
	}

	return MyVariant();
}

std::string SQLNormalRecord::strValue(const std::string &fieldName)
{
	SQLNormalTableSchema *tableSchema = static_cast<SQLNormalTableSchema *>(m_table->schema());
	uint32_t offset = tableSchema->dataOffSet(fieldName);
	int fieldIndex = tableSchema->fieldIndex(fieldName);
	auto &mm = MemoryManager::instantce(m_table->threadIndex());
	auto &memOpr = mm.arrayMemory().memoryOperator(m_dataId);
	if (isNull(fieldIndex)) {
		return "NULL";
	}

	FieldSchema* fd = tableSchema->field(fieldIndex);
	switch (fd->dataType())
	{
		case DataType::dtBoolean:
			return memOpr.getInt8(offset) == 1 ? "true" : "false";
		case DataType::dtSmallInt:
			return to_string(memOpr.getInt16(offset));
		case DataType::dtInt:
			return to_string(memOpr.getInt32(offset));
		case DataType::dtBigInt:
			return to_string(memOpr.getInt64(offset));
		case DataType::dtFloat:
		case DataType::dtDouble:
			return to_string(memOpr.getFloat64(offset));
		case DataType::dtString:
		{
			uint32_t varId = memOpr.getUint32(offset);
			if (varId == 0) {
				return string();
			}

			VarData vData = mm.varMemory().get(varId);
			return string((char *)vData.data, vData.len);
		}
		default:
			break;
	}

	return "";
}

void SQLNormalRecord::save(WriteBuffer* buffer)
{
	auto tableSchema = static_cast<SQLNormalTableSchema *>(m_table->schema());
	MemoryManager::instantce(m_table->threadIndex()).arrayMemory().memoryOperator(m_dataId);
	writeNullBit(buffer, tableSchema);
	for (int i = 0; i < tableSchema->fieldCount(); ++i) {
		FieldSchema *fd = tableSchema->field(i);
		writeField(buffer, fd, tableSchema->dataOffSet(fd->name()));
	}
}

void SQLNormalRecord::read(std::unordered_map<std::string, MyVariant>& sqlResult, 
	bool directColumnName)
{
	auto tableSchema = static_cast<SQLNormalTableSchema *>(m_table->schema());
	MemoryManager::instantce(m_table->threadIndex()).arrayMemory().memoryOperator(m_dataId);
	for (int i = 0; i < tableSchema->fieldCount(); ++i) {
		FieldSchema *field = tableSchema->field(i);
		readField(sqlResult, field, directColumnName ? field->name() :
			tableSchema->getRealColumnName(field->name()));
	}
}

void SQLNormalRecord::readField(std::unordered_map<std::string, MyVariant>& sqlResult, 
	FieldSchema *field, const std::string &sqlFieldName)
{
	auto& mm = MemoryManager::instantce(m_table->threadIndex());
	auto& memOpr = mm.arrayMemory().memoryOperator();

	auto tableSchema = static_cast<SQLNormalTableSchema*>(m_table->schema());
	int fieldIndex = tableSchema->fieldIndex(field->name());
	if (sqlResult[sqlFieldName].isNull()) {
		setNull(fieldIndex, true);
		return;
	}
	
	setNull(fieldIndex, false);
	uint32_t offset = tableSchema->dataOffSet(field->name());
	switch (field->dataType())
	{
		case DataType::dtBoolean:
		{
			memOpr.setInt8(offset, sqlResult[sqlFieldName].toBool() ? 1 : 0);
			break;
		}
		case DataType::dtSmallInt:
		{
			memOpr.setInt16(offset, sqlResult[sqlFieldName].toInt());
			break;
		}
		case DataType::dtInt:
		{
			memOpr.setInt32(offset, sqlResult[sqlFieldName].toInt());
			break;
		}
		case DataType::dtBigInt:
		{
			memOpr.setInt64(offset, sqlResult[sqlFieldName].toInt64());
			break;
		}
		case DataType::dtFloat:
		case DataType::dtDouble:
		{
			memOpr.setFloat64(offset, sqlResult[sqlFieldName].toDouble());
			break;
		}
		case DataType::dtString:
		{
			string strValue = sqlResult[sqlFieldName].toString();
			if (strValue.empty()) {
				memOpr.setUint32(offset, 0);
			}
			else {
				uint32_t varId = mm.varMemory().set(VarData((uint8_t *)strValue.c_str(), strValue.size()));
				memOpr.setUint32(offset, varId);
			}
			break;
		}
		case DataType::dtBlob:
		{
			ByteArray blobValue = sqlResult[sqlFieldName].toBlob();
			if (blobValue->byteLength() == 0) {
				memOpr.setUint32(offset, 0);
			}
			else {
				uint32_t varId = mm.varMemory().set(VarData(blobValue->data(), blobValue->byteLength()));
				memOpr.setUint32(offset, varId);
			}
			break;
		}
		default:
			break;
	}
}

void SQLNormalRecord::writeNullBit(WriteBuffer* buffer, SQLNormalTableSchema* tableSchema)
{
	auto& memOpr = MemoryManager::instantce(
		m_table->threadIndex()).arrayMemory().memoryOperator();

	if (tableSchema->kind() == TableKind::tkExtend) {
		auto extendTableSchema = static_cast<SQLExtendTableSchema*>(tableSchema);
		int byteCount = (tableSchema->fieldCount() - 1) / 8 + 1;
		ByteArray bitBytes = ByteArray::from(byteCount);

		int baseFieldCount = tableSchema->fieldCount() - extendTableSchema->extendFieldCount();
		int baseByteCount = (baseFieldCount - 1) / 8 + 1;
		ByteArray baseBytes = memOpr.getBytes(0, baseByteCount);
		bitBytes->assign(baseBytes);
		ByteArray eBitBytes = memOpr.getBytes(extendTableSchema->extendOffSet(), 
			(extendTableSchema->extendFieldCount() - 1) / 8 + 1);
		int8_t bitIndex = 8 - baseByteCount * 8 - baseFieldCount;
		int32_t byteIndex = baseByteCount;
		int32_t srcByteIndex = 0;
		int8_t srcBitIndex = 0;
		for (int i = 0; i < extendTableSchema->extendFieldCount(); ++i) {
			if (bitIndex == 8) {
				++byteIndex;
				bitIndex = 0;
			}

			if (srcBitIndex == 8) {
				++srcByteIndex;
				srcBitIndex = 0;
			}

			bitBytes->setBit(byteIndex, bitIndex++, eBitBytes->getBit(srcByteIndex, srcBitIndex++));
		}

		buffer->writeBytes(bitBytes);
	}
	else {
		int byteCount = (tableSchema->fieldCount() - 1) / 8 + 1;
		buffer->writeBytes(memOpr.getBytes(0, byteCount));
	}
}

void SQLNormalRecord::setNull(int fieldIndex, bool value)
{
	auto& memOpr = MemoryManager::instantce(m_table->threadIndex()).arrayMemory().memoryOperator();
	auto tableSchema = static_cast<SQLNormalTableSchema*>(m_table->schema());
	if (tableSchema->kind() == TableKind::tkExtend) {
		auto exTableSchema = static_cast<SQLExtendTableSchema*>(tableSchema);
		if (fieldIndex < tableSchema->fieldCount() - exTableSchema->extendFieldCount()) {
			int8_t byteIndex = fieldIndex / 8;
			memOpr.setBit(byteIndex, fieldIndex - byteIndex * 8, value ? 1 : 0);
		}
		else {
			int8_t exFieldIndex = fieldIndex - (tableSchema->fieldCount() - exTableSchema->extendFieldCount());
			int8_t byteIndex = exFieldIndex / 8;
			memOpr.setBit(exTableSchema->extendOffSet() + byteIndex, exFieldIndex - byteIndex * 8, value ? 1 : 0);
		}
	}
	else {
		int8_t byteIndex = fieldIndex / 8;
		memOpr.setBit(byteIndex, fieldIndex - byteIndex * 8, value ? 1 : 0);
	}
}

bool SQLNormalRecord::isNull(int fieldIndex) const
{
	auto& memOpr = MemoryManager::instantce(m_table->threadIndex()).arrayMemory().memoryOperator();
	auto tableSchema = static_cast<SQLNormalTableSchema*>(m_table->schema());
	if (tableSchema->kind() == TableKind::tkExtend) {
		auto exTableSchema = static_cast<SQLExtendTableSchema*>(tableSchema);
		int normalFieldCount = tableSchema->fieldCount() - exTableSchema->extendFieldCount();
		if (fieldIndex < normalFieldCount) {
			int8_t byteIndex = fieldIndex / 8;
			return memOpr.getBit(byteIndex, fieldIndex - byteIndex * 8) == 1;
		}
		else {
			int8_t exFieldIndex = fieldIndex - normalFieldCount;
			int8_t byteIndex = exFieldIndex / 8;
			return memOpr.getBit(exTableSchema->extendOffSet() + byteIndex, exFieldIndex - byteIndex * 8) == 1;
		}
	}
	else {
		int8_t byteIndex = fieldIndex / 8;
		return memOpr.getBit(byteIndex, fieldIndex - byteIndex * 8) == 1;
	}
}

void SQLNormalRecord::writeField(WriteBuffer* buffer, FieldSchema *field, uint32_t offset)
{
	auto &mm = MemoryManager::instantce(m_table->threadIndex());
	auto &memOpr = mm.arrayMemory().memoryOperator();
	switch (field->dataType())
	{
		case DataType::dtBoolean:
		{
			buffer->writeByte(memOpr.getInt8(offset));
			break;
		}
		case DataType::dtSmallInt:
		{
			buffer->writeShort(memOpr.getInt16(offset));
			break;
		}
		case DataType::dtInt:
		{
			buffer->writeInt(memOpr.getInt32(offset));
			break;
		}
		case DataType::dtBigInt:
		{
			buffer->writeLong(memOpr.getInt64(offset));
			break;
		}
		case DataType::dtFloat:
		case DataType::dtDouble:
		{
			buffer->writeDouble(memOpr.getFloat64(offset));
			break;
		}
		case DataType::dtString:
		{
			uint32_t varId = memOpr.getUint32(offset);
			if (varId == 0) {
				buffer->writeUShort(0);
			}
			else {
				VarData vData = mm.varMemory().get(varId);
				buffer->writeString(string((char *)vData.data, vData.len));
			}
			break;
		}
		case DataType::dtBlob:
		{
			uint32_t varId = memOpr.getUint32(offset);
			if (varId == 0) {
				buffer->writeUInt(0);
			}
			else {
				VarData vData = mm.varMemory().get(varId);
				buffer->writeBlock(ByteArray::from(vData.data, vData.len));
			}
			break;
		}
		default:
			break;
	}
}

uint32_t SQLNormalRecord::dataId() const
{
	return m_dataId;
}

SQLReadOnlyTable::SQLReadOnlyTable(SQLTableSchema *schema) :
	SQLTable(schema)
{
}

SQLReadOnlyTable::~SQLReadOnlyTable()
{
	for (int i = 0; i < m_recs.size(); ++i) {
		delete m_recs[i];
	}
}

TableKind SQLReadOnlyTable::kind() const
{
	return TableKind::tkReadOnly;
}

SQLRecord *SQLReadOnlyTable::newRecord()
{
	SQLRecord *rec = new SQLNormalRecord(this);
	return rec;
}

SQLRecord *SQLReadOnlyTable::append(SQLRecord *rec)
{
	m_recs.push_back(static_cast<SQLNormalRecord *>(rec));
	return rec;
}

int SQLReadOnlyTable::recordCount() const
{
	return m_recs.size();
}

void SQLReadOnlyTable::forEach(const ForEachRecordEvent &e)
{
	for (int i = 0; i < m_recs.size(); ++i) {
		e(m_recs[i]);
	}
}

void SQLReadOnlyTable::doSave(WriteBuffer* buffer)
{
	SQLTable::doSave(buffer);
	buffer->writeInt(m_recs.size());
	for (int i = 0; i < m_recs.size(); ++i) {
		m_recs[i]->save(buffer);
	}
}

void SQLReadOnlyTable::doUnload(OutputStream &out)
{
	SQLTable::doUnload(out);
	out.writeInt(m_recs.size());
	for (int i = 0; i < m_recs.size(); ++i) {
		out.writeUInt(m_recs[i]->dataId());
	}
}

void SQLReadOnlyTable::doLoad(InputStream &in)
{
	SQLTable::doLoad(in);
	int32_t count = in.readInt();
	for (int i = 0; i < count; ++i) {
		uint32_t dataId = in.readUInt();
		m_recs.push_back(new SQLNormalRecord(this, dataId));
	}
}

SQLExtendRecord::SQLExtendRecord(SQLRecord *base) :
	SQLRecord(base ? base->table() : nullptr),
	m_base(base)
{
}

SQLExtendRecord::~SQLExtendRecord()
{
}

void SQLExtendRecord::setBase(SQLRecord *base)
{
	m_base = base;
	if (m_base) {
		m_table = m_base->table();
	}
}

void SQLExtendRecord::addMapFields(const std::string &srcFieldName, const std::string &dstFieldName)
{
	m_fieldMap[srcFieldName] = dstFieldName;
}

const MyVariant SQLExtendRecord::value(const std::string &fieldName) const
{
	auto f = m_fieldMap.find(fieldName);
	if (f != m_fieldMap.end()) {
		return m_base->value(f->second);
	}

	return m_base->value(fieldName);
}

void SQLExtendRecord::setValue(const std::string &fieldName, const MyVariant &value)
{
	m_base->setValue(fieldName, value);
}

int64_t SQLExtendRecord::pk() const
{
	return m_base->pk();
}

SQLTempRecord::SQLTempRecord(SQLTable* table) :
	SQLRecord(table)
{
	m_offsets = new int32_t[static_cast<SQLTempTable *>(table)->normalSchema()->fieldCount()];
}

SQLTempRecord::~SQLTempRecord()
{
	delete[] m_offsets;
}

const MyVariant SQLTempRecord::value(const std::string &fieldName) const
{
	SQLNormalTableSchema* tableSchema = static_cast<SQLNormalTableSchema*>(m_table->schema());
	int fieldIndex = tableSchema->fieldIndex(fieldName);
	if (fieldIndex < 0) {
		return MyVariant();
	}

	if (isNull(fieldIndex)) {
		return MyVariant();
	}

	FieldSchema* fd = tableSchema->field(fieldIndex);
	int32_t offset = m_offsets[fieldIndex];
	auto data = static_cast<SQLTempTable*>(m_table)->data();
	switch (fd->dataType())
	{
		case DataType::dtBoolean:
			return data->getInt8(offset) == 1 ? true : false;
		case DataType::dtSmallInt:
			return data->getInt16(offset);
		case DataType::dtInt:
			return data->getInt32(offset);
		case DataType::dtBigInt:
			return data->getInt64(offset);
		case DataType::dtFloat:
		case DataType::dtDouble:
			return data->getFloat64(offset);
		case DataType::dtString:
		{
			uint16_t len = data->getUint16(offset);
			std::string value;
			value.resize(len, 0);
			for (int i = 0; i < len; ++i) {
				value[i] = (char)data->getUint8(offset + 2 + i);
			}
			return value;
		}
		case DataType::dtBlob:
		{
			int32_t len = data->getInt32(offset);
			return data->slice(offset + 4, offset + 4 + len);
		}
		default:
			break;
	}

	return MyVariant();
}

std::string SQLTempRecord::strValue(const std::string& fieldName)
{
	SQLNormalTableSchema* tableSchema = static_cast<SQLNormalTableSchema*>(m_table->schema());
	int fieldIndex = tableSchema->fieldIndex(fieldName);
	if (fieldIndex < 0 || isNull(fieldIndex)) {
		return "NULL";
	}

	FieldSchema* fd = tableSchema->field(fieldIndex);
	int32_t offset = m_offsets[fieldIndex];
	auto data = static_cast<SQLTempTable*>(m_table)->data();
	switch (fd->dataType())
	{
		case DataType::dtBoolean:
			return data->getInt8(offset) == 1 ? "true" : "false";
		case DataType::dtSmallInt:
			return to_string(data->getInt16(offset));
		case DataType::dtInt:
			return to_string(data->getInt32(offset));
		case DataType::dtBigInt:
			return to_string(data->getInt64(offset));
		case DataType::dtFloat:
		case DataType::dtDouble:
			return to_string(data->getFloat64(offset));
		case DataType::dtString:
		{
			uint16_t len = data->getUint16(offset);
			std::string value;
			value.resize(len, 0);
			for (int i = 0; i < len; ++i) {
				value[i] = (char)data->getUint8(offset + 2 + i);
			}
			return value;
		}
		default:
			break;
	}

	return "";
}

void SQLTempRecord::setValue(const std::string& fieldName, const MyVariant& value)
{
	// do nothing
}

int64_t SQLTempRecord::pk() const
{
	return static_cast<SQLTempTable*>(m_table)->intPK(this);
}

void SQLTempRecord::load(InputStream &in)
{
	SQLNormalTableSchema* tableSchema = static_cast<SQLNormalTableSchema*>(m_table->schema());
	int fieldCount = tableSchema->fieldCount();
	int normalFieldCount = fieldCount;
	if (tableSchema->kind() == TableKind::tkExtend) {
		normalFieldCount -= static_cast<SQLExtendTableSchema*>(tableSchema)->extendFieldCount();
	}

	int nullBytes = (normalFieldCount - 1) / 8 + 1;
	in.skip(nullBytes);
	int32_t offset = in.pos();
	for (int i = 0; i < fieldCount; ++i) {
		if (i == normalFieldCount) {
			// extend Field area
			nullBytes = (fieldCount - normalFieldCount - 1) / 8 + 1;
			in.skip(nullBytes);
		}

		m_offsets[i] = offset;
		FieldSchema* fd = tableSchema->field(i);
		switch (fd->dataType())
		{
		case DataType::dtBoolean:
		{
			offset += 1;
			in.skip(1);
			break;
		}
		case DataType::dtSmallInt:
		{
			offset += 2;
			in.skip(2);
			break;
		}
		case DataType::dtInt:
		{
			offset += 4;
			in.skip(4);
			break;
		}
		case DataType::dtBigInt:
		{
			offset += 8;
			in.skip(8);
			break;
		}
		case DataType::dtFloat:
		case DataType::dtDouble:
		{
			offset += 8;
			in.skip(8);
			break;
		}
		case DataType::dtString:
		{
			uint16_t len = in.readUShort();
			offset += 2 + len;
			in.skip(len);
			break;
		}
		case DataType::dtBlob:
		{
			int len = in.readInt();
			offset += 4 + len;
			in.skip(len);
			break;
		}
		default:
			break;
		}
	}
}

bool SQLTempRecord::isNull(int fieldIndex) const
{
	auto tableSchema = static_cast<SQLNormalTableSchema*>(m_table->schema());
	auto data = static_cast<SQLTempTable*>(m_table)->data();
	if (tableSchema->kind() == TableKind::tkExtend) {
		auto exTableSchema = static_cast<SQLExtendTableSchema*>(tableSchema);
		int normalFieldCount = tableSchema->fieldCount() - exTableSchema->extendFieldCount();
		if (fieldIndex < normalFieldCount) {
			int32_t normalOffSet = m_offsets[0] - (normalFieldCount - 1) / 8 - 1;
			int8_t byteIndex = fieldIndex / 8;
			return data->getBit(normalOffSet + byteIndex, fieldIndex - byteIndex * 8) == 1;
		}
		else {
			int8_t exFieldIndex = fieldIndex - normalFieldCount;
			int8_t byteIndex = exFieldIndex / 8;
			return data->getBit(exTableSchema->extendOffSet() + byteIndex, exFieldIndex - byteIndex * 8) == 1;
		}
	}
	else {
		int32_t offSet = m_offsets[0] - (tableSchema->fieldCount() - 1) / 8 - 1;
		int8_t byteIndex = fieldIndex / 8;
		return data->getBit(offSet + byteIndex, fieldIndex - byteIndex * 8) == 1;
	}
}

SQLTempTable::SQLTempTable(SQLTableSchema* schema) :
	SQLTable(schema)
{
}

SQLTempTable::~SQLTempTable()
{
	for (int i = 0; i < m_recs.size(); ++i) {
		delete m_recs[i];
	}
}

SQLRecord* SQLTempTable::newRecord()
{
	SQLRecord* rec = new SQLTempRecord(this);
	return rec;
}

SQLRecord* SQLTempTable::append(SQLRecord* rec)
{
	m_recs.push_back(rec);
	return rec;
}

int SQLTempTable::recordCount() const
{
	return m_recs.size();
}

void SQLTempTable::forEach(const ForEachRecordEvent& e)
{
	for (int i = 0; i < m_recs.size(); ++i) {
		e(m_recs[i]);
	}
}

FieldSchema* SQLTempTable::primaryKey() const
{
	return normalSchema()->primaryKey();
}

int64_t SQLTempTable::intPK(const SQLRecord *rec) const
{
	FieldSchema* pkField = primaryKey();
	if (!pkField) {
		return 0;
	}

	return rec->value(pkField->name()).toInt64();
}

SQLNormalTableSchema* SQLTempTable::normalSchema() const
{
	return static_cast<SQLNormalTableSchema*>(m_schema);
}

std::string SQLTempTable::name() const
{
	return m_schema->name();
}

ByteArrayImpl* SQLTempTable::data() const
{
	return m_data;
}

void SQLTempTable::setData(ByteArrayImpl *data)
{
	m_data = data;
}
