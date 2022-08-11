package de.samply.common.ldmclient.cql;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.DataFormatException;
import com.google.common.base.Splitter;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import de.samply.common.ldmclient.LdmClientCqlQuery;
import de.samply.common.ldmclient.LdmClientException;
import de.samply.common.ldmclient.LdmClientUtil;
import de.samply.common.ldmclient.model.LdmQueryResult;
import de.samply.share.model.common.Error;
import de.samply.share.model.common.QueryResultStatistic;
import de.samply.share.model.common.result.Stratification;
import de.samply.share.model.common.result.Stratum;
import de.samply.share.model.cql.CqlResult;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.Configurable;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.hl7.fhir.r4.model.CapabilityStatement;
import org.hl7.fhir.r4.model.MeasureReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client to communicate with the local datamanagement implementation "Samply Blaze".
 */
public class LdmClientCql extends LdmClientCqlQuery<CqlResult, CqlResult, Error> {

  private static final String EVALUATE = "$evaluate-measure?periodStart=2000&periodEnd=2019";
  private static final FhirContext FHIR_CONTEXT = FhirContext.forR4();
  private static final Logger logger = LoggerFactory.getLogger(LdmClientCql.class);

  public LdmClientCql(CloseableHttpClient httpClient, String ldmBaseUrl) {
    super(httpClient, ldmBaseUrl);
  }

  @Override
  protected Class<CqlResult> getResultClass() {
    return CqlResult.class;
  }

  @Override
  protected Class<CqlResult> getStatisticsClass() {
    return CqlResult.class;
  }

  @Override
  protected Class<Error> getErrorClass() {
    return Error.class;
  }

  @Override
  public String getUserAgentInfo() {
    return "Blaze/" + getVersionString();
  }

  @Override
  public CqlResult getResult(String location) {
    throw new NotImplementedException(
        "CqlResult depends on two locations and must be created differently.");
  }

  @Override
  public String getVersionString() {
    HttpGet httpGet = new HttpGet(getLdmBaseUrl() + "metadata");
    try (CloseableHttpResponse response = getHttpClient().execute(httpGet)) {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode == HttpStatus.SC_OK) {
        HttpEntity entity = response.getEntity();
        String entityOutput = EntityUtils.toString(entity, Consts.UTF_8);
        CapabilityStatement capabilityStatement = (CapabilityStatement) FHIR_CONTEXT.newJsonParser()
            .parseResource(entityOutput);
        return capabilityStatement.getSoftware().getVersion();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return "unknown";
  }

  @Override
  public LdmQueryResult getStatsOrError(String location) throws LdmClientException {
    HttpGet httpGet = new HttpGet(LdmClientUtil.addTrailingSlash(location) + EVALUATE);
    CloseableHttpClient httpClient = getHttpClient();
    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode == HttpStatus.SC_OK) {
        HttpEntity entity = response.getEntity();
        String entityOutput = EntityUtils.toString(entity, Consts.UTF_8);
        MeasureReport measureReport = (MeasureReport) FHIR_CONTEXT.newJsonParser()
            .parseResource(entityOutput);

        QueryResultStatistic queryResultStatistic = createQueryResultStatistic(measureReport);

        return new LdmQueryResult(queryResultStatistic);
      } else if (statusCode >= 400 && statusCode < 500) {
        Error error = createError(statusCode);
        return new LdmQueryResult(error);
      } else if (statusCode == HttpStatus.SC_GATEWAY_TIMEOUT) {
        throw new LdmClientException(
            "Gateway timeout while evaluating measure with URL '" + location + "'");
      } else {
        throw new LdmClientException(
            "Unexpected response code '" + statusCode + "' while evaluating measure with URL '"
                + location + "'");
      }
    } catch (SocketTimeoutException e) {
      throw new LdmClientException(
          "Timeout (" + ((Configurable) httpClient).getConfig().getSocketTimeout()
              + " ms) while evaluating measure with URL '" + location + "'", e);
    } catch (DataFormatException | IOException e) {
      throw new LdmClientException(e);
    }
  }

  private Error createError(int statusCode) {
    Error error = new Error();
    error.setErrorCode(statusCode);
    return error;
  }

  private QueryResultStatistic createQueryResultStatistic(MeasureReport measureReport) {
    MeasureReport.MeasureReportGroupComponent firstGroup = measureReport.getGroupFirstRep();
    int count = firstGroup.getPopulationFirstRep().getCount();

    QueryResultStatistic queryResultStatistic = new QueryResultStatistic();
    queryResultStatistic.setTotalSize(count);
    queryResultStatistic.setNumberOfPages(1);
    queryResultStatistic.setRequestId("1");

    for (MeasureReport.MeasureReportGroupStratifierComponent fhirStratifier : firstGroup
        .getStratifier()) {
      Stratification stratification = new Stratification();
      stratification.setTitle(fhirStratifier.getCodeFirstRep().getText());
      for (MeasureReport.StratifierGroupComponent fhirStratum : fhirStratifier.getStratum()) {
        Stratum stratum = new Stratum();
        stratum.setLabel(fhirStratum.getValue().getText());
        stratum.setCount(fhirStratum.getPopulationFirstRep().getCount());
        stratification.getStrata().add(stratum);
      }

      queryResultStatistic.getStratification().add(stratification);
    }

    return queryResultStatistic;
  }

  /**
   * Create a subject list for an entity.
   *
   * @param location the measure URI
   * @return location url for the subject list
   * @throws LdmClientException exception which can be thrown while posting the query to the ldm
   */
  public String createSubjectList(URI location) throws LdmClientException {
    URI measureReportUri = createMeasureReport(location);
    return getFirstSubjectListUri(measureReportUri);
  }


  URI createMeasureReport(URI location) throws LdmClientException {
    JsonObject parameter = loadParameterStub();
    location = URI.create(location + "/$evaluate-measure");
    HttpPost httpPost = new HttpPost(location);
    httpPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/fhir+json");
    HttpEntity entity = new StringEntity(parameter.toString(), Consts.UTF_8);
    httpPost.setEntity(entity);
    try (CloseableHttpResponse response = getHttpClient().execute(httpPost)) {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode != HttpStatus.SC_CREATED) {
        logger.error(String.format("Measure report not created. Status code: %d, Response: %s",
            statusCode, EntityUtils.toString(response.getEntity(), Consts.UTF_8)));
        throw new LdmClientException(
            "Measure report not created. Received status code " + statusCode);
      }
      Header locationHeader = response.getFirstHeader("Location");
      if (locationHeader == null) {
        throw new LdmClientException("Location header is missing");
      }
      return resourceLocation(locationHeader.getValue());
    } catch (IOException | URISyntaxException e) {
      throw new LdmClientException(e);
    }
  }

  String getFirstSubjectListUri(URI measureReportUri) throws LdmClientException {
    HttpGet httpGet = new HttpGet(measureReportUri);
    httpGet.setHeader(HttpHeaders.ACCEPT, "application/fhir+json");
    try (CloseableHttpResponse response = getHttpClient().execute(httpGet)) {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode == HttpStatus.SC_OK) {
        HttpEntity entity = response.getEntity();
        String entityOutput = EntityUtils.toString(entity, Consts.UTF_8);
        MeasureReport measureReport = (MeasureReport) FHIR_CONTEXT.newJsonParser()
            .parseResource(entityOutput);
        return LdmClientUtil.addTrailingSlash(getLdmBaseUrl()) + measureReport.getGroupFirstRep()
            .getPopulationFirstRep()
            .getSubjectResults()
            .getReference();
      } else {
        throw new LdmClientException(
            "Unexpected response code '" + statusCode
                + "' while getting subject list from measure with URL '"
                + measureReportUri + "'");
      }
    } catch (IOException e) {
      throw new LdmClientException(e);
    }
  }

  private static URI resourceLocation(String location) throws URISyntaxException {
    Iterator<String> partIter = Splitter.on("/_history").split(location).iterator();
    return new URI(partIter.next());
  }

  private static JsonObject loadParameterStub() {
    return loadJson("parameters-stub.json");
  }

  private static JsonObject loadJson(String name) {
    InputStream in = LdmClientCql.class.getResourceAsStream(name);
    return new JsonParser().parse(new InputStreamReader(in, StandardCharsets.UTF_8))
        .getAsJsonObject();
  }

  public boolean isLdmCql() {
    return true;
  }
}
