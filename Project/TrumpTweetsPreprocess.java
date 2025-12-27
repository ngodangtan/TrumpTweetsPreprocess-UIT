import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Pattern;

public class TrumpTweetsPreprocess extends Configured implements Tool {

  // =========================
  // Counters Job 1: Preprocess
  // =========================
  public enum PREP_COUNTER {
    TOTAL_LINES,
    HEADER_LINES,
    PARSE_ERROR,
    RETWEET_DROPPED,
    EMPTY_TEXT_DROPPED,
    DATE_INVALID_DROPPED,
    VALID_OUTPUT
  }

  // =========================
  // Counters Job 2: Dedup
  // =========================
  public enum DEDUP_COUNTER {
    INPUT_LINES,
    OUTPUT_UNIQUE,
    DUPLICATE_DROPPED
  }

  // =========================
  // Regex cleaning
  // =========================
  private static final Pattern URL = Pattern.compile("https?://\\S+|www\\.\\S+");
  private static final Pattern MENTION = Pattern.compile("@\\w+");
  private static final Pattern HASHTAG = Pattern.compile("#\\w+");
  private static final Pattern NON_LETTER = Pattern.compile("[^a-z\\s]");
  private static final Pattern MULTI_SPACE = Pattern.compile("\\s+");

  // =========================
  // Stopwords (basic)
  // =========================
  private static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList(
      "a","an","the","and","or","but","if","then","else","for","to","of","in","on","at","by",
      "is","am","are","was","were","be","been","being","it","its","this","that","these","those",
      "i","you","he","she","we","they","me","my","your","our","their","him","her","them",
      "as","with","from","about","into","over","under","again","very","more","most","so","too"
  ));

  // =========================
  // CSV parser (handles commas in quotes)
  // =========================
  private static List<String> parseCsvLine(String line) {
    List<String> out = new ArrayList<>();
    if (line == null) return out;

    StringBuilder cur = new StringBuilder();
    boolean inQuotes = false;

    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);

      if (c == '"') {
        if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
          cur.append('"');
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (c == ',' && !inQuotes) {
        out.add(cur.toString());
        cur.setLength(0);
      } else {
        cur.append(c);
      }
    }
    out.add(cur.toString());
    return out;
  }

  private static String safeTrim(String s) { return s == null ? "" : s.trim(); }

  // =========================
  // Missing data: number parse -> 0
  // =========================
  private static long parseLongOrZero(String s) {
    try {
      s = safeTrim(s);
      if (s.isEmpty()) return 0L;
      return Long.parseLong(s);
    } catch (Exception e) {
      return 0L;
    }
  }

  // =========================
  // isRetweet: supports true/1/yes/y + t (your CSV uses t/f)
  // =========================
  private static boolean parseBooleanLoose(String s) {
    s = safeTrim(s).toLowerCase();
    return s.equals("true") || s.equals("1") || s.equals("yes") || s.equals("y") || s.equals("t");
  }

  // =========================
  // Normalize date -> YYYY-MM-DD (string)
  // =========================
  private static String normalizeDate(String raw) {
    raw = safeTrim(raw);
    if (raw.length() >= 10) return raw.substring(0, 10);
    return raw;
  }

  // =========================
  // Validate date truly ISO (YYYY-MM-DD)
  // =========================
  private static boolean isValidISODate(String yyyyMmDd) {
    try {
      LocalDate.parse(yyyyMmDd, DateTimeFormatter.ISO_LOCAL_DATE);
      return true;
    } catch (DateTimeParseException e) {
      return false;
    }
  }

  // =========================
  // Clean + normalize text
  // =========================
  private static String cleanText(String text) {
    text = safeTrim(text).toLowerCase();
    if (text.isEmpty()) return "";

    // Data Cleaning
    text = URL.matcher(text).replaceAll(" ");
    text = MENTION.matcher(text).replaceAll(" ");
    text = HASHTAG.matcher(text).replaceAll(" ");

    // remove special chars/emoji/punctuation
    text = NON_LETTER.matcher(text).replaceAll(" ");

    // Data Normalization
    text = MULTI_SPACE.matcher(text).replaceAll(" ").trim();
    if (text.isEmpty()) return "";

    // tokenization (split by space) + remove stopwords
    StringBuilder sb = new StringBuilder();
    for (String w : text.split(" ")) {
      if (w.isEmpty()) continue;
      if (STOPWORDS.contains(w)) continue;
      sb.append(w).append(' ');
    }
    return sb.toString().trim();
  }

  // =========================================================
  // JOB 1: Preprocess
  // Input CSV header:
  // id,text,isRetweet,isDeleted,device,favorites,retweets,date,isFlagged
  // indexes:
  // 0 ,1   ,2       ,3        ,4     ,5        ,6      ,7   ,8
  // Output line format:
  // date,likes,retweets,clean_text
  // =========================================================
  public static class PreprocessMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    // Default index exactly matches your raw CSV
    private int IDX_TEXT = 1;
    private int IDX_IS_RETWEET = 2;
    private int IDX_LIKES = 5;    // favorites
    private int IDX_RETWEETS = 6;
    private int IDX_DATE = 7;

    @Override
    protected void setup(Context context) {
      // optional override
      Configuration conf = context.getConfiguration();
      IDX_DATE = conf.getInt("tweet.idx.date", IDX_DATE);
      IDX_TEXT = conf.getInt("tweet.idx.text", IDX_TEXT);
      IDX_LIKES = conf.getInt("tweet.idx.likes", IDX_LIKES);
      IDX_RETWEETS = conf.getInt("tweet.idx.retweets", IDX_RETWEETS);
      IDX_IS_RETWEET = conf.getInt("tweet.idx.is_retweet", IDX_IS_RETWEET);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      context.getCounter(PREP_COUNTER.TOTAL_LINES).increment(1);

      String line = value.toString();
      if (line == null || line.trim().isEmpty()) {
        context.getCounter(PREP_COUNTER.PARSE_ERROR).increment(1);
        return;
      }

      // drop header
      String lower = line.toLowerCase();
      if (lower.startsWith("id,text,isretweet")) {
        context.getCounter(PREP_COUNTER.HEADER_LINES).increment(1);
        return;
      }

      List<String> cols = parseCsvLine(line);
      int need = Math.max(
          Math.max(IDX_DATE, IDX_TEXT),
          Math.max(Math.max(IDX_LIKES, IDX_RETWEETS), IDX_IS_RETWEET)
      );

      if (cols.size() <= need) {
        context.getCounter(PREP_COUNTER.PARSE_ERROR).increment(1);
        return;
      }

      try {
        String textRaw = cols.get(IDX_TEXT);
        String isRtRaw = cols.get(IDX_IS_RETWEET);
        String likesRaw = cols.get(IDX_LIKES);
        String retweetsRaw = cols.get(IDX_RETWEETS);
        String dateRaw = cols.get(IDX_DATE);

        // 3.4 Data Cleaning: remove retweets
        if (parseBooleanLoose(isRtRaw)) {
          context.getCounter(PREP_COUNTER.RETWEET_DROPPED).increment(1);
          return;
        }

        // 3.6 Missing Data: likes/retweets missing -> 0
        long likes = parseLongOrZero(likesRaw);
        long retweets = parseLongOrZero(retweetsRaw);

        // 3.5 Normalize date -> yyyy-mm-dd
        String date = normalizeDate(dateRaw);

        // 3.6 Invalid date -> drop
        if (!isValidISODate(date)) {
          context.getCounter(PREP_COUNTER.DATE_INVALID_DROPPED).increment(1);
          return;
        }

        // 3.4 + 3.5 clean/normalize text
        String clean = cleanText(textRaw);

        // 3.6 Missing text -> drop
        if (clean.isEmpty()) {
          context.getCounter(PREP_COUNTER.EMPTY_TEXT_DROPPED).increment(1);
          return;
        }

        String out = date + "," + likes + "," + retweets + "," + clean;
        context.write(NullWritable.get(), new Text(out));
        context.getCounter(PREP_COUNTER.VALID_OUTPUT).increment(1);

      } catch (Exception e) {
        context.getCounter(PREP_COUNTER.PARSE_ERROR).increment(1);
      }
    }
  }

  public static class IdentityReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text v : values) context.write(NullWritable.get(), v);
    }
  }

  // =========================================================
  // JOB 2: Deduplicate
  // Input: date,likes,retweets,clean_text
  // Key: clean_text (remove duplicates by same cleaned content)
  // Reducer aggregate:
  // - date: choose MIN date (earliest)
  // - likes: choose MAX likes
  // - retweets: choose MAX retweets
  // Output: date,likes,retweets,clean_text (unique)
  // =========================================================
  public static class DedupMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      context.getCounter(DEDUP_COUNTER.INPUT_LINES).increment(1);

      String line = value.toString();
      if (line == null || line.trim().isEmpty()) return;

      // split into 4 parts only, clean_text may contain commas? (it doesn't, by our cleaning)
      String[] parts = line.split(",", 4);
      if (parts.length != 4) return;

      String date = parts[0];
      String likes = parts[1];
      String retweets = parts[2];
      String cleanText = parts[3];

      // key = clean_text, value = date,likes,retweets
      context.write(new Text(cleanText), new Text(date + "," + likes + "," + retweets));
    }
  }

  public static class DedupReducer extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    public void reduce(Text cleanText, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      String minDate = null;
      long maxLikes = Long.MIN_VALUE;
      long maxRetweets = Long.MIN_VALUE;

      int count = 0;
      for (Text v : values) {
        count++;
        String[] p = v.toString().split(",", 3);
        if (p.length != 3) continue;

        String date = p[0];
        long likes = parseLongOrZero(p[1]);
        long retweets = parseLongOrZero(p[2]);

        if (minDate == null || date.compareTo(minDate) < 0) minDate = date;
        if (likes > maxLikes) maxLikes = likes;
        if (retweets > maxRetweets) maxRetweets = retweets;
      }

      if (minDate == null) return;

      if (count > 1) {
        context.getCounter(DEDUP_COUNTER.DUPLICATE_DROPPED).increment(count - 1);
      }
      context.getCounter(DEDUP_COUNTER.OUTPUT_UNIQUE).increment(1);

      String out = minDate + "," + maxLikes + "," + maxRetweets + "," + cleanText.toString();
      context.write(NullWritable.get(), new Text(out));
    }
  }

  // =========================================================
  // ToolRunner: run 2 jobs sequentially
  // Usage: TrumpTweetsPreprocess <input> <output>
  // It will create temp output automatically: <output>_tmp
  // =========================================================
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: TrumpTweetsPreprocess <input> <output>");
      return 2;
    }

    Configuration conf = getConf();
    String input = args[0];
    String output = args[1];
    String tmp = output + "_tmp";

    Path inputPath = new Path(input);
    Path tmpPath = new Path(tmp);
    Path outPath = new Path(output);

    // Clean existing tmp/out (avoid "already exists")
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(tmpPath)) fs.delete(tmpPath, true);
    if (fs.exists(outPath)) fs.delete(outPath, true);

    // -------------------------
    // Job 1: Preprocess
    // -------------------------
    Job job1 = Job.getInstance(conf, "Trump Tweets Preprocess - Job1");
    job1.setJarByClass(TrumpTweetsPreprocess.class);

    job1.setMapperClass(PreprocessMapper.class);
    job1.setReducerClass(IdentityReducer.class);

    job1.setMapOutputKeyClass(NullWritable.class);
    job1.setMapOutputValueClass(Text.class);
    job1.setOutputKeyClass(NullWritable.class);
    job1.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job1, inputPath);
    FileOutputFormat.setOutputPath(job1, tmpPath);

    boolean ok1 = job1.waitForCompletion(true);
    if (!ok1) return 1;

    System.out.println("===== JOB1 COUNTERS (Preprocess) =====");
    for (PREP_COUNTER c : PREP_COUNTER.values()) {
      long v = job1.getCounters().findCounter(c).getValue();
      System.out.println(c.name() + ": " + v);
    }
    System.out.println("=====================================");

    // -------------------------
    // Job 2: Deduplicate
    // -------------------------
    Job job2 = Job.getInstance(conf, "Trump Tweets Dedup - Job2");
    job2.setJarByClass(TrumpTweetsPreprocess.class);

    job2.setMapperClass(DedupMapper.class);
    job2.setReducerClass(DedupReducer.class);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setOutputKeyClass(NullWritable.class);
    job2.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job2, tmpPath);
    FileOutputFormat.setOutputPath(job2, outPath);

    boolean ok2 = job2.waitForCompletion(true);
    if (!ok2) return 1;

    System.out.println("===== JOB2 COUNTERS (Dedup) =====");
    for (DEDUP_COUNTER c : DEDUP_COUNTER.values()) {
      long v = job2.getCounters().findCounter(c).getValue();
      System.out.println(c.name() + ": " + v);
    }
    System.out.println("================================");

    // Optional: remove tmp to keep workspace clean
    if (fs.exists(tmpPath)) fs.delete(tmpPath, true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new TrumpTweetsPreprocess(), args);
    System.exit(exitCode);
  }
}
