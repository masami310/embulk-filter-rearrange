package org.embulk.filter;

import java.util.ArrayList;
import java.util.List;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;

public class RearrangeFilterPlugin implements FilterPlugin {
	
	private static final Logger logger = Exec.getLogger(RearrangeFilterPlugin.class);
	
	public RearrangeFilterPlugin() {
	}
	
	public interface PluginTask extends Task {
		@Config("columns")
		public List<ColumnConfig> getColumns();
		
		@Config("output_data")
		public List<List<String>> getOutputData();
		
		@Config("add_row_number")
		@ConfigDefault("false")
		public boolean getAddRowNumber();
		
		@Config("row_number_column_name")
		@ConfigDefault("\"row_number\"")
		public String getRowNumberColumnName();
	}

	@Override
	public void transaction(ConfigSource config, Schema inputSchema, Control control) {
		PluginTask task = config.loadConfig(PluginTask.class);
		
		List<ColumnConfig> columnConfigs = task.getColumns();
		ImmutableList.Builder<Column> builder = ImmutableList.builder();
		int index = 0;
		for (ColumnConfig columnConfig : columnConfigs) {
			Column column = new Column(index++, columnConfig.getName(), columnConfig.getType());
			builder.add(column);
		}
		
		boolean addRowNumber = task.getAddRowNumber();
		if (addRowNumber) {
			String rowNumberColumnName = task.getRowNumberColumnName();
			Column column = new Column(index++, rowNumberColumnName, Types.LONG);
			builder.add(column);
		}
		
		Schema outputSchema = new Schema(builder.build());
		control.run(task.dump(), outputSchema);
	}
	
	@Override
	public PageOutput open(
			TaskSource taskSource,
			final Schema inputSchema,
			final Schema outputSchema,
			final PageOutput output) {
		
		PluginTask task = taskSource.loadTask(PluginTask.class);
		final boolean addRowNumber = task.getAddRowNumber();
		
		List<List<String>> outputData = task.getOutputData();
		final List<List<Column>> resultList = new ArrayList<List<Column>>();
		
		for (int rowNum = 0; rowNum < outputData.size(); rowNum++) {
			List<String> outputRow = outputData.get(rowNum);
			List<Column> result = new ArrayList<Column>();
			
			int colMax = outputSchema.getColumnCount() - (addRowNumber ? 1 : 0);
			
			boolean outputInfo = false;
			for (int colNum = 0; colNum < colMax; colNum++) {
				if (colNum < outputRow.size()) {
					String outputCell = outputRow.get(colNum);
					Column inputColumn = inputSchema.lookupColumn(outputCell); 
					if (inputColumn != null) {
						Type inputType = inputColumn.getType();
						Type outputType = outputSchema.getColumnType(colNum);
						if (inputType.equals(outputType)) {
							result.add(inputColumn);
							
						} else {
							logger.warn(
									String.format(
											"Type unmatched: columns[%d].type = %s, output_data[%d][%d].type = %s",
											colNum,
											inputType.toString(),
											rowNum,
											colNum,
											outputType.toString()));
							result.add(null);
						}
						
					} else {
						logger.warn(String.format("%s is not exist in input schema.", outputCell));
						result.add(null);
					}
					
				} else {
					if (!outputInfo) {
						logger.info(
								String.format(
										"The length of output_data[%d] (%d) is less than a length of columns (%d). So we fill remain output columns to null.",
										rowNum,
										outputRow.size(),
										colMax));
						outputInfo = true;
					}
					
					result.add(null);
				}
			}
			
			resultList.add(result);
		}
		
		return new PageOutput() {
			
			private PageReader pageReader = new PageReader(inputSchema);
			private PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

			@Override
			public void finish() {
				pageBuilder.finish();
			}

			@Override
			public void close() {
				pageBuilder.close();
			}
			
			@Override
			public void add(Page page) {
				pageReader.setPage(page);
				
				while (pageReader.nextRecord()) {
					for (int rowNum = 0; rowNum < resultList.size(); rowNum++) {
						List<Column> result = resultList.get(rowNum);
						for (int colNum = 0; colNum < result.size(); colNum++) {
							Column column = result.get(colNum);
							if (column != null) {
								if (column.getType() instanceof BooleanType) {
									pageBuilder.setBoolean(colNum, pageReader.getBoolean(column));
									
								} else if (column.getType() instanceof LongType) {
									pageBuilder.setLong(colNum, pageReader.getLong(column));
									
								} else if (column.getType() instanceof DoubleType) {
									pageBuilder.setDouble(colNum, pageReader.getDouble(column));
									
								} else if (column.getType() instanceof StringType) {
									pageBuilder.setString(colNum, pageReader.getString(column));
									
								} else if (column.getType() instanceof TimestampType) {
									pageBuilder.setTimestamp(colNum, pageReader.getTimestamp(column));
									
								} else {
									pageBuilder.setNull(colNum);
								}
								
							} else {
								pageBuilder.setNull(colNum);
							}
						}
						
						if (addRowNumber) {
							pageBuilder.setLong(outputSchema.getColumnCount() - 1, rowNum);
						}
						
						pageBuilder.addRecord();
					}
				}
			}
		};
	}
}
