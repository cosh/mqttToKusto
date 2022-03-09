namespace mqttToKusto
{
    public class SettingsKusto
    {
        public string ClientId { get; set; }
        public string ClientSecret { get; set; }
        public string TenantId { get; set; }
        public string ClusterName { get; set; }
        public string DbName { get; set; }
        public int MaxRetries { get; set; }
        public int MsBetweenRetries { get; set; }
    }
}