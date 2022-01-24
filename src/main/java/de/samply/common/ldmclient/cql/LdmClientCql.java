package de.samply.common.ldmclient.cql;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.DataFormatException;
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
import java.net.SocketTimeoutException;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.Configurable;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.hl7.fhir.r4.model.CapabilityStatement;
import org.hl7.fhir.r4.model.MeasureReport;

/**
 * Client to communicate with the local datamanagement implementation "Samply Blaze".
 */
public class LdmClientCql extends LdmClientCqlQuery<CqlResult, CqlResult, Error> {

  private static final String EVALUATE = "$evaluate-measure?periodStart=2000&periodEnd=2019";
  private static final FhirContext FHIR_CONTEXT = FhirContext.forR4();

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
      if (stratification.getTitle().equals("Custodian")){
        for (MeasureReport.StratifierGroupComponent fhirStratum : fhirStratifier.getStratum()) {
          Stratum stratum = new Stratum();
          stratum.setLabel(fhirStratum.getValue().getText());
          stratum.setCount(fhirStratum.getPopulationFirstRep().getCount());
          stratification.getStrata().add(stratum);
        }
      }

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

  public boolean isLdmCql() {
    return true;
  }
}
