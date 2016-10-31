Embulk::JavaPlugin.register_output(
  "salesforce_bulk", "org.embulk.output.salesforce_bulk.SalesforceBulkOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
