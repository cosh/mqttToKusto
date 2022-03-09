using System.Collections.Generic;

namespace mqttToKusto
{
    public class Settings
    {
        public string APPINSIGHTS_INSTRUMENTATIONKEY { get; set; }
        public string MqttURL { get; set; }
        public string MqttClientId { get; set; }
        public SettingsKusto Kusto { get; set; } = null!;

        public List<SettingsSubscription> Subscriptions { get; set; } = null!;
    }
}