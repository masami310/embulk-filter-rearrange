Embulk::JavaPlugin.register_filter(
  "rearrange", "org.embulk.filter.RearrangeFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
