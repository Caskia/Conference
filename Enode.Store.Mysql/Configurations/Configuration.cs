using ENode.Configurations;
using ENode.Eventing;
using ENode.Infrastructure;
using Sparxo.Enode.Eventing.Impl;
using Sparxo.Enode.Infrastructure.Impl;

namespace Enode.Store.Mysql.Configurations
{
    public static class ENodeConfigurationExtension
    {
        public static ENodeConfiguration UseMySqlEventStore(this ENodeConfiguration enodeConfiguration, OptionSetting optionSetting = null)
        {
            enodeConfiguration.GetCommonConfiguration().SetDefault<IEventStore, MySqlEventStore>(new MySqlEventStore(optionSetting));
            return enodeConfiguration;
        }

        public static ENodeConfiguration UseMySqlLockService(this ENodeConfiguration enodeConfiguration, OptionSetting optionSetting = null)
        {
            enodeConfiguration.GetCommonConfiguration().SetDefault<ILockService, MySqlLockService>(new MySqlLockService(optionSetting));
            return enodeConfiguration;
        }

        public static ENodeConfiguration UseMySqlPublishedVersionStore(this ENodeConfiguration enodeConfiguration, OptionSetting optionSetting = null)
        {
            enodeConfiguration.GetCommonConfiguration().SetDefault<IPublishedVersionStore, MySqlPublishedVersionStore>(new MySqlPublishedVersionStore(optionSetting));
            return enodeConfiguration;
        }
    }
}