package com.ganqiang.recsys.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ganqiang.recsys.entity.UserActionLog;

public final class JsonHelper {


	public static UserActionLog format(String jsons) {
		UserActionLog loginfo = new UserActionLog();

		ObjectMapper mapper = new ObjectMapper();
		JsonNode root;
		try {
			root = mapper.readTree(jsons);
			JsonNode data = root.path("data");
			JsonNode access_time = data.path("access_time");
			JsonNode item_id = data.path("item_id");
			JsonNode from = root.path("from");
			JsonNode url = root.path("url");
			JsonNode session_id = root.path("session_id");
			JsonNode cookies = root.path("cookies");
			JsonNode language = root.path("language");
			JsonNode bs = root.path("bs");
			JsonNode client_id = root.path("client_id");
			JsonNode action = root.path("action");
			loginfo.setAccessTime(access_time.asText());
			loginfo.setItemId(item_id.asText());
			loginfo.setBs(bs.asText());
			loginfo.setClientId(client_id.asText());
			loginfo.setCookies(cookies.asText());
			loginfo.setFrom(from.asText());
			loginfo.setSessionId(session_id.asText());
			loginfo.setUrl(url.asText());
			loginfo.setAction(action.asText());
			loginfo.setLanguage(language.asText());
		} catch (Exception e) {
			e.printStackTrace();
		}

		return loginfo;
	}
	
	public static void main(String... args){
		UserActionLog log = format(Constants.test_log);
		System.out.println(log.getAccessTime());
	}

}
