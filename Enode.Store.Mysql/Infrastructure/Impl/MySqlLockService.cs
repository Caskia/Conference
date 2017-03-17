using Dapper;
using ECommon.Utilities;
using ENode.Configurations;
using ENode.Infrastructure;
using MySql.Data.MySqlClient;
using Sparxo.Dapper;
using System;
using System.Data;
using System.Linq;

namespace Sparxo.Enode.Infrastructure.Impl
{
    public class MySqlLockService : ILockService
    {
        #region Private Variables

        private readonly string _connectionString;
        private readonly string _lockKeySqlFormat;
        private readonly string _tableName;

        #endregion Private Variables

        #region Constructors

        public MySqlLockService(OptionSetting optionSetting)
        {
            if (optionSetting != null)
            {
                _connectionString = optionSetting.GetOptionValue<string>("ConnectionString");
                _tableName = optionSetting.GetOptionValue<string>("TableName");
            }
            else
            {
                var setting = ENodeConfiguration.Instance.Setting.DefaultDBConfigurationSetting;
                _connectionString = setting.ConnectionString;
                _tableName = setting.LockKeyTableName;
            }

            Ensure.NotNull(_connectionString, "_connectionString");
            Ensure.NotNull(_tableName, "_tableName");

            _lockKeySqlFormat = "SELECT * FROM " + _tableName + " WHERE `Name` = '{0}' FOR UPDATE";
        }

        #endregion Constructors

        public void AddLockKey(string lockKey)
        {
            using (var connection = GetConnection())
            {
                var count = connection.QueryList(new { Name = lockKey }, _tableName).Count();
                if (count == 0)
                {
                    connection.Insert(new { Name = lockKey }, _tableName);
                }
            }
        }

        public void ExecuteInLock(string lockKey, Action action)
        {
            using (var connection = GetConnection())
            {
                connection.Open();
                var transaction = connection.BeginTransaction();
                try
                {
                    LockKey(transaction, lockKey);
                    action();
                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
            }
        }

        private IDbConnection GetConnection()
        {
            return new MySqlConnection(_connectionString);
        }

        private void LockKey(IDbTransaction transaction, string key)
        {
            var sql = string.Format(_lockKeySqlFormat, key);
            transaction.Connection.Query(sql, transaction: transaction);
        }
    }
}