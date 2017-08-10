/*
 * Copyright 2017 Emmanuel Keller / JAEKSOFT
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jaeksoft.loader.dbpedia;

import com.opensearchserver.client.JsonClient1;
import com.opensearchserver.client.common.LanguageEnum;
import com.opensearchserver.client.common.update.DocumentUpdate;
import com.opensearchserver.client.v1.UpdateApi1;
import com.opensearchserver.client.v1.field.FieldUpdate;
import com.qwazr.cli.Option;
import com.qwazr.cli.Parser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class ShortAbstractLoader extends TtlLoader implements Consumer<TtlLineReader> {

	public static class Arguments {

		@Option(desc = "URL of the short_abstract file", argName = "url", hasArg = true)
		public final String abstractUrl = null;

		@Option(desc = "Path of the short_abstract file", argName = "file", hasArg = true)
		public final String abstractPath = null;

		@Option(desc = "URL of the OpenSearchServer instance", argName = "string", hasArg = true)
		public final String instanceUrl = null;

		@Option(desc = "Name of the index to fill", argName = "string", hasArg = true)
		public final String indexName = null;

		@Option(desc = "The optional login", argName = "string", hasArg = true)
		public final String login = null;

		@Option(desc = "The optional key", argName = "string", hasArg = true)
		public final String key = null;

		@Option(desc = "Buffer size", argName = "integer", hasArg = true)
		public final Integer bufferSize = null;

		@Option(desc = "Language", argName = "string", hasArg = true)
		public final String language = null;

		final static String DEFAULT_SHORT_ABSTRACT_PATH = "data/short_abstracts_en.ttl.bz2";

		File getAbstractFile() {
			return new File(abstractPath == null ? DEFAULT_SHORT_ABSTRACT_PATH : abstractPath);
		}

		final static String DEFAULT_SHORT_ABSTRACT_URL =
				"http://downloads.dbpedia.org/3.9/en/short_abstracts_en.ttl.bz2";

		URL getAbstractURL() throws MalformedURLException {
			return new URL(abstractUrl == null ? DEFAULT_SHORT_ABSTRACT_URL : abstractUrl);
		}

		final static int DEFAULT_BUFFER_SIZE = 1000;

		int getBufferSize() {
			return bufferSize == null ? DEFAULT_BUFFER_SIZE : bufferSize;
		}

		final static LanguageEnum DEFAULT_LANGUAGE = LanguageEnum.ENGLISH;

		LanguageEnum getLanguage() {
			return language == null ? DEFAULT_LANGUAGE : LanguageEnum.findByCode(language);
		}

	}

	static final Logger logger = Logger.getLogger(ShortAbstractLoader.class.getName());

	static void checkAbstractFile(URL shortAbstractUrl, File shortAbstractFile) throws IOException {

		// Download the DBPedia file
		if (!shortAbstractFile.exists()) {
			shortAbstractFile.getParentFile().mkdir();
			try (final InputStream input = shortAbstractUrl.openStream()) {
				try (ReadableByteChannel rbc = Channels.newChannel(input)) {
					try (final FileOutputStream fos = new FileOutputStream(shortAbstractFile)) {
						try (final FileChannel fileChannel = fos.getChannel()) {
							fileChannel.transferFrom(rbc, 0, Long.MAX_VALUE);
						}
					}
				}
			}
		}
	}

	private final JsonClient1 jsonClient;
	private final UpdateApi1 updateApi;
	private final String indexName;
	private final List<DocumentUpdate> buffer;
	private final int bufferSize;
	private final LanguageEnum language;

	ShortAbstractLoader(JsonClient1 jsonClient, URL shortAbstractUrl, File shortAbstractFile, String indexName,
			int bufferSize, LanguageEnum language) throws IOException {
		super(shortAbstractFile);
		this.jsonClient = jsonClient;
		this.updateApi = new UpdateApi1(jsonClient);
		this.buffer = new ArrayList<>();
		this.indexName = indexName;
		this.bufferSize = bufferSize;
		this.language = language;
		checkAbstractFile(shortAbstractUrl, shortAbstractFile);
		load(Integer.MAX_VALUE, this);
		flush();
	}

	synchronized void flush() {
		if (buffer.isEmpty())
			return;
		try {
			updateApi.updateDocuments(indexName, buffer);
		} catch (IOException | URISyntaxException e) {
			throw new RuntimeException(e);
		}
		logger.info(buffer.size() + " documents");
		buffer.clear();
	}

	public static void main(String[] args)
			throws IOException, URISyntaxException, NoSuchMethodException, ParseException, InstantiationException,
			IllegalAccessException {
		final Arguments arguments = new Parser<>(Arguments.class).parse(args);
		new ShortAbstractLoader(new JsonClient1(arguments.instanceUrl, arguments.login, arguments.key, 10 * 60 * 1000),
				arguments.getAbstractURL(), arguments.getAbstractFile(), arguments.indexName, arguments.getBufferSize(),
				arguments.getLanguage());
	}

	@Override
	public void accept(TtlLineReader ttlLineReader) {
		if (StringUtils.isEmpty(ttlLineReader.object))
			return;
		final String[] parts = StringUtils.split(ttlLineReader.subject, '/');
		if (parts == null || parts.length == 0)
			return;
		final String urlReplace = "https://" + language.getCode() + ".wikipedia.org/wiki/";
		final String url = ttlLineReader.subject.replace("http://dbpedia.org/resource/", urlReplace).replace(
				"http://" + language.getCode() + ".dbpedia.org/resource/", urlReplace);
		final String title = parts[parts.length - 1].replace('_', ' ');

		final DocumentUpdate documentUpdate = new DocumentUpdate();
		documentUpdate.setLang(language);
		documentUpdate.addField(new FieldUpdate("url", url, 1.0f));
		documentUpdate.addField(new FieldUpdate("title", title + " - Wikipedia", 1.0f));
		documentUpdate.addField(new FieldUpdate("content", ttlLineReader.object, 1.0f));
		documentUpdate.addField(new FieldUpdate("contentBaseType", "text/html", 1.0f));
		documentUpdate.addField(new FieldUpdate("host", language.getCode() + ".wikipedia.org", 1.0f));
		documentUpdate.addField(new FieldUpdate("lang", language.getCode(), 1.0f));
		buffer.add(documentUpdate);
		if (buffer.size() >= bufferSize)
			flush();
	}
}
