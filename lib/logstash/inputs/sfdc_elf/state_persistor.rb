# encoding: utf-8
require 'date'

# Handel what the next procedure should be based on the .sfdc_info_logstash file. States proceed via reading and
# writing LogDates to the .sfdc_info_logstash file.
class StatePersistor
  LOG_KEY        = 'SFDC - StatePersistor'
  FILE_PREFIX    = 'sfdc_info_logstash'
  DEFAULT_TIME   = '0001-01-01T00:00:00Z'

  def initialize(logger, base_path, org_id)
    @logger = logger
    @last_indexed_date_path = "#{base_path}/.#{FILE_PREFIX}_last_indexed_#{org_id}"
    @read_logs_path = "#{base_path}/.#{FILE_PREFIX}_read_#{org_id}"
  end

  public
  def has_last_indexed_file?
    File.exist?(@last_indexed_date_path)
  end

  # Read the last indexed LogDate from .sfdc_info_logstash file and return it. If the .sfdc_info_logstash file does
  # not exist then create the file and write DEFAULT_TIME to it using update_last_indexed_log_date() method.

  public
  def get_last_indexed_log_date
    # Read from .sfdc_info_logstash if it exists, otherwise load @last_read_log_date with DEFAULT_TIME.
    if has_last_indexed_file?
      # Load last read LogDate from .sfdc_info_logstash.
      @logger.info("#{LOG_KEY}: .#{@last_indexed_date_path} does exist, read and return the time on it.")
      File.read(@last_indexed_date_path)
    else
      # Load default time to ensure getting all possible EventLogFiles from oldest to current. Also
      # create .sfdc_info_logstash file
      @logger.info("#{LOG_KEY}: .sfdc_info_logstash does not exist and loaded DEFAULT_TIME to @last_read_instant")
      update_last_indexed_log_date(DEFAULT_TIME)
      DEFAULT_TIME
    end
  end




  # Take as input a date sting that is in iso8601 format, then overwrite .sfdc_info_logstash with the date string,
  # because of the 'w' flag used with the File class.
  public 
  def add_log_read(result_row)
    @logger.info("#{LOG_KEY}: adding #{result_row.EventType},#{result_row.LogDate} to #{@read_logs_path}")
    f = File.open(@read_logs_path, 'a')
    f.write("#{result_row.EventType},#{result_row.LogDate}\n")
    f.flush
    f.close
  end

  public
  def update_last_indexed_log_date(date)
    @logger.info("#{LOG_KEY}: overwriting #{@last_indexed_date_path} with #{date}")
    f = File.open(@last_indexed_date_path, 'w')
    f.write(date)
    f.flush
    f.close
  end

  public
  def log_read(result_row)
    last_indexed_date = self.get_last_indexed_log_date
    if self.has_last_indexed_file? && DateTime.parse(result_row.LogDate) <= DateTime.parse(last_indexed_date)
      matcher = Regexp.escape("#{result_row.EventType},#{result_row.LogDate}")
      return File.readlines(@read_logs_path).grep(/#{matcher}/).size > 0
    end
    false
  end
end
