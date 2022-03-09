namespace mqttToKusto
{
    internal class IngestionJob
    {
        public string ToBeIngested { get; internal set; }
        public SettingsSubscription Subcription { get; internal set; }
    }
}