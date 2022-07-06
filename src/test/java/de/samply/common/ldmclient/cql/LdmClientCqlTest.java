package de.samply.common.ldmclient.cql;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import de.samply.common.ldmclient.LdmClientException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LdmClientCqlTest {

  private static final String BASE_URL = "localhost";

  @Mock
  private CloseableHttpClient httpClient;

  private LdmClientCql ldmClientCql;

  @BeforeEach
  void setUp() {
    ldmClientCql = new LdmClientCql(httpClient, BASE_URL);
  }

  @Test
  void testCreateMeasureReport() throws IOException, URISyntaxException, LdmClientException {
    String measureReportUri = "/fhir/Measure/DALRE7BKVA6HY3IK";
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    when(httpClient.execute(argThat(httpPostMatcher(measureReportUri + "/$evaluate-measure")))).thenReturn(response);
    when(response.getStatusLine()).thenReturn(statusLine(201));
    when(response.getFirstHeader("Location")).thenReturn(httpHeader());
    URI location = ldmClientCql.createMeasureReport(new URI(BASE_URL + measureReportUri));
    assertEquals(BASE_URL + "/fhir/MeasureReport/DAOSEDN3UFW4FXHB", location.getPath());
  }

  @Test
  void testGetFirstSubjectListUri() throws IOException, URISyntaxException, LdmClientException {
    String subjectListUri = "/fhir/MeasureReport/DAOSEDN3UFW4FXHB";
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    when(httpClient.execute(argThat(httpGetMatcher(subjectListUri)))).thenReturn(response);
    when(response.getStatusLine()).thenReturn(statusLine(200));
    when(response.getEntity()).thenReturn(httpEntity(loadJson("measureReport.json").toString()));
    String location = ldmClientCql.getFirstSubjectListUri(new URI(BASE_URL + subjectListUri));
    assertEquals(BASE_URL + "/List/DAOSEDNXBYQKUMRU", location);
  }

  private static ArgumentMatcher<HttpPost> httpPostMatcher(String uri) {
    return httpPost -> URI.create(BASE_URL + uri).equals(httpPost.getURI());
  }

  private static ArgumentMatcher<HttpGet> httpGetMatcher(String uri) {
    return httpGet -> URI.create(BASE_URL + uri).equals(httpGet.getURI());
  }

  private static BasicStatusLine statusLine(int statusCode) {
    return new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), statusCode, "");
  }

  private BasicHeader httpHeader() {
    return new BasicHeader("Location",
        BASE_URL + "/fhir/MeasureReport/DAOSEDN3UFW4FXHB/_history/333");
  }

  private BasicHttpEntity httpEntity(String content) {
    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(new ByteArrayInputStream(content.getBytes(UTF_8)));
    return entity;
  }

  private static JsonObject loadJson(String name) {
    InputStream in = LdmClientCqlTest.class.getResourceAsStream(name);
    return new JsonParser().parse(new InputStreamReader(in, StandardCharsets.UTF_8))
        .getAsJsonObject();
  }

}
