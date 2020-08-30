package com.redis.dataflowstreaming;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.HashMap;

@SpringBootApplication
public class DataflowStreamingApplication {
	private static final Logger LOG = LoggerFactory.getLogger(DataflowStreamingApplication.class);
	private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(10);
	public static interface WordCountOptions extends PipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://bucket-test-demo/*.txt")
		String getInputFile();
		void setInputFile(String value);

	}

	public static void main(String[] args) {

		WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(WordCountOptions.class);

		Pipeline p = Pipeline.create(options);

		p.apply(
				"ReadLines", TextIO.read() .from(options.getInputFile())
						.watchForNewFiles(DEFAULT_POLL_INTERVAL, Watch.Growth.<String>never() ))
				.apply(
						"Transform Record",                     // the transform name
						ParDo.of( new DoFn<String, String[]> () {    // a DoFn as an anonymous inner class instance
							@ProcessElement
							public void processElement(@Element String line, OutputReceiver<String[]> out) {
								LOG.info("line content: "+line);
								String[] fields = line.split("\\|");
								out.output(fields);
							}
						})).apply(
				"Transform Record",                     // the transform name

				ParDo.of(new DoFn<String[], KV<String, String>>() {    // a DoFn as an anonymous inner class instance
					@ProcessElement
					public void processElement(@Element String[] fields, OutputReceiver<KV<String, String>> out) {

						HashMap< String,String> hmap = new HashMap<String, String>();
						hmap.put("guid",fields[0]);
						hmap.put("firstName",fields[1]);
						hmap.put("middleName",fields[2]);
						hmap.put("lastName",fields[3]);
						hmap.put("dob",fields[4]);
						hmap.put("postalCode",fields[5]);
						hmap.put("gender",fields[6]);
						hmap.put("pid",fields[7]);


						if(fields[1]!=null) {
							out.output(KV.of("hash1:".concat(hmap.get("firstName")), hmap.get("guid")));
							out.output(KV.of("hash2:".concat(hmap.get("middleName")), hmap.get("guid")));
							out.output(KV.of("hash3:".concat(hmap.get("lastName")), hmap.get("guid")));
							out.output(KV.of("hash4:".concat(hmap.get("dob")), hmap.get("guid")));
							out.output(KV.of("hash5:".concat(hmap.get("postalCode")), hmap.get("guid")));
							out.output(KV.of("hash6:".concat(hmap.get("gender")), hmap.get("guid")));
							out.output(KV.of("hash11:".concat(hmap.get("guid")), "hash12:".concat(hmap.get("pid"))));
						}
					}
				})).apply( RedisIO.write().withMethod(RedisIO.Write.Method.SADD).withEndpoint("localhost", 6379));

		p.run();
	}
}
