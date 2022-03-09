namespace mqttToKusto
{
    public class SettingsSubscription
    {
        public string MqttTopic { get; set; }
        public string KustoTable { get; set; }
        public string MappingName { get; set; }

        public int BatchLimitInMinutes { get; set; }

        public int BatchLimitNumberOfEvents { get; set; }
    }
}