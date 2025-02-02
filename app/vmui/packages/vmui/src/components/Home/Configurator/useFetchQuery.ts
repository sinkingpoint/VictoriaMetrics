import {useEffect, useMemo, useState} from "react";
import {getQueryRangeUrl, getQueryUrl} from "../../../api/query-range";
import {useAppState} from "../../../state/common/StateContext";
import {InstantMetricResult, MetricResult} from "../../../api/types";
import {saveToStorage} from "../../../utils/storage";
import {isValidHttpUrl} from "../../../utils/url";
import {useAuthState} from "../../../state/auth/AuthStateContext";
import {TimeParams} from "../../../types";

export const useFetchQuery = (): {
  fetchUrl?: string,
  isLoading: boolean,
  graphData?: MetricResult[],
  liveData?: InstantMetricResult[],
  error?: string,
} => {
  const {query, displayType, serverUrl, time: {period}, queryControls: {nocache}} = useAppState();

  const {basicData, bearerData, authMethod} = useAuthState();

  const [isLoading, setIsLoading] = useState(false);
  const [graphData, setGraphData] = useState<MetricResult[]>();
  const [liveData, setLiveData] = useState<InstantMetricResult[]>();
  const [error, setError] = useState<string>();
  const [prevPeriod, setPrevPeriod] = useState<TimeParams>();

  useEffect(() => {
    if (error) {
      setGraphData(undefined);
      setLiveData(undefined);
    }
  }, [error]);

  const needUpdateData = useMemo(() => {
    if (!prevPeriod) return true;
    const duration = (prevPeriod.end - prevPeriod.start) / 3;
    const factorLimit = duration / (period.end - period.start) >= 0.7;
    const maxLimit = period.end > (prevPeriod.end + duration);
    const minLimit = period.start < (prevPeriod.start - duration);
    return factorLimit || maxLimit || minLimit;
  }, [period]);

  const fetchData = async () => {
    if (!fetchUrl) return;
    setIsLoading(true);
    setPrevPeriod(period);

    const headers = new Headers();
    if (authMethod === "BASIC_AUTH") {
      headers.set("Authorization", "Basic " + btoa(`${basicData?.login || ""}:${basicData?.password || ""}`));
    }
    if (authMethod === "BEARER_AUTH") {
      headers.set("Authorization", bearerData?.token || "");
    }

    try {
      const response = await fetch(fetchUrl, { headers });
      if (response.ok) {
        saveToStorage("LAST_QUERY", query);
        const resp = await response.json();
        setError(undefined);
        displayType === "chart" ? setGraphData(resp.data.result) : setLiveData(resp.data.result);
      } else {
        setError((await response.json())?.error);
      }
    } catch (e) {
      if (e instanceof Error) setError(e.message);
    }

    setIsLoading(false);
  };

  const fetchUrl = useMemo(() => {
    if (!period) return;
    if (!serverUrl) {
      setError("Please enter Server URL");
    } else if (!query.trim()) {
      setError("Please enter a valid Query and execute it");
    } else if (isValidHttpUrl(serverUrl)) {
      const duration = (period.end - period.start) / 2;
      const bufferPeriod = {...period, start: period.start - duration, end: period.end + duration};
      return displayType === "chart"
        ? getQueryRangeUrl(serverUrl, query, bufferPeriod, nocache)
        : getQueryUrl(serverUrl, query, period);
    } else {
      setError("Please provide a valid URL");
    }
  },
  [serverUrl, period, displayType]);

  useEffect(() => {
    setPrevPeriod(undefined);
  }, [query]);

  // TODO: this should depend on query as well, but need to decide when to do the request.
  //       Doing it on each query change - looks to be a bad idea. Probably can be done on blur
  useEffect(() => {
    fetchData();
  }, [serverUrl, displayType]);

  useEffect(() => {
    if (needUpdateData) {
      fetchData();
    }
  }, [period]);

  return {
    fetchUrl,
    isLoading,
    graphData,
    liveData,
    error
  };
};
