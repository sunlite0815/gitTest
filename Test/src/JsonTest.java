import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Joiner;

public class JsonTest {
    protected final static ObjectMapper mapper = new ObjectMapper();
    
    public static final String NAVI_LOG_V2_CAMPAIGN         = "action_body.campaign";
    
    @SuppressWarnings({ "unchecked", "unused" })
    public static void main(String[] args) {
        BufferedReader reader = null;
//        String json = "{\"@timestamp\":\"2023-04-16T23:36:32.017Z\",\"stb_mac\":\"30:eb:25:1d:57:b6\",\"device_model\":\"BID-AT200\",\"vod_watch_type\":\"\",\"manufacturer\":\"INTEK\",\"page_id\":\"/my\",\"page_path\":\"||MY\",\"page_type\":\"web\",\"log_type\":\"dev\",\"action_id\":\"click.contents_list.selection\",\"client_ip\":\"192.168.0.29\",\"service_name\":\"btv_catv\",\"session_id\":\"2020111809194276603006090793\",\"poc_type\":\"stb_app\",\"url\":\"\",\"log_time\":\"2023-04-16T23:36:28.809Z\",\"server_received_time\":\"20230417083628.809\",\"web_page_version\":\"5.3.6.187\",\"stb_id\":\"33C9E913-F70C-11EA-BD09-5F5CE34E524C\",\"stb_fw_version\":\"15.536.8-0000\",\"app_build_version\":\"22428\",\"os_name\":\"Android\",\"os_version\":\"10\",\"pcid\":\"20201118091942766030060\",\"client_ip_logserver\":\"218.237.104.92\",\"device_base_time\":\"20230417083618.151\",\"menu_id\":\"NM2000017213\",\"source\":\"/home/manager/logs/naviLog/naviLog-2023-04-17-08.log\",\"offset\":35494,\"action_body\":{\"campaign\":\"\",\"menu_name\":\"\",\"block_id\":\"NM2000017219\",\"result\":\"\",\"target\":\"\",\"group\":\"stb\",\"block_name\":\"����\",\"category\":\"�ý��� ����\",\"voice\":\"\",\"position\":\"\",\"menu_id\":\"\"},\"host\":{\"name\":\"BMT-VM-BDP-NAVILOG-01\"},\"contents_body\":{},\"beat\":{\"name\":\"BMT-VM-BDP-NAVILOG-01\",\"hostname\":\"BMT-VM-BDP-NAVILOG-01\",\"version\":\"6.4.2\"},\"edid\":{\"manufacturer_id\":\"SAM\",\"manufacturer_week\":\"47/2017\",\"max_resolution\":\"1920x1080p 60Hz\",\"monitor_name\":\"SAMSUNG\",\"version\":\"1.3\"},\"prospector\":{\"type\":\"log\"},\"member\":{\"nickname\":\"�츮��TV1\",\"profile_type\":\"btv\",\"profile_id\":\"a2f1b153-f17e-4e10-b54e-002aaddfbc94\"},\"remote_control\":{},\"fields\":{\"log_topic\":\"topic-navilog\"},\"race\":{},\"@metadata\":{\"beat\":\"filebeat\",\"type\":\"doc\",\"version\":\"6.4.2\",\"topic\":\"topic-navilog\"}}";
        String json = "";
        Map<String, Object> source = null;
        try {
            reader = new BufferedReader(new FileReader("C:\\SKB\\99.TEMP\\json3.txt"));
            while ((json = reader.readLine()) != null) {
                System.out.println(json);
                byte[] body = json.getBytes();
                Map<String, Object> jsonMap = mapper.readValue(body, Map.class);
                
                source = makeFlatMap(jsonMap);

                boolean isCampaign = false;
                List<String> campaign = null;
                
                try {
                    Object objCamp = source.get(NAVI_LOG_V2_CAMPAIGN);

                    if (objCamp instanceof List<?>) {
                        campaign = (List<String>) objCamp;
                    } else if (objCamp instanceof String) {
                        String strCamp = (String) objCamp;
                        if (!"".equals(strCamp)) {
                            System.out.println(strCamp);
                            campaign = new ArrayList<String>();
                            if (strCamp.startsWith("[")) {
                                campaign.add(strCamp.replace("[", "").replace("]", ""));
                            } else if (strCamp.startsWith("{")) {
                                campaign.add(strCamp);
                            } else {
                                String [] info = strCamp.split(",");
                                
                                if (info.length == 3) {
                                    StringBuffer camp = new StringBuffer();
                                    camp.append("{");
                                    
                                    for (int i = 0; i < info.length; i++) {
                                        if (i > 0) {
                                            camp.append(",");
                                        }
                                        if (info[i].startsWith("C")) {
                                            camp.append("\"camp_id\":\"").append(info[i]).append("\""); 
                                        } else if (info[i].startsWith("N")) {
                                            camp.append("\"chnl_node_id\":\"").append(info[i]).append("\"");
                                        } else {
                                            camp.append("\"camp_exec_no\":\"").append(info[i]).append("\"");
                                        }
                                    }
                                    
                                    camp.append("}");
                                    campaign.add(camp.toString());
                                }
                            }
//                            campaign.add(strCamp);
//                            Map<String, Object> campMap = makeFlatMap(mapper.readValue(((String) objCamp), Map.class), NAVI_LOG_V2_CAMPAIGN);
                        } else {
                            System.out.println("호롤롤롤로");
                        }
                    }
//                    campaign = (List<String>) source.get("action_body.campaign.test");
//                  campaign = (List<String>) source.get(NAVI_LOG_V2_CAMPAIGN);
                } catch (ClassCastException e) {
                    isCampaign = false;
                    campaign = null;
                }
                isCampaign = (campaign != null && campaign.size() > 0);

                final String notNullColumns = "action_id,stb_id";
                if( !isCampaign ){
                    if( notNullColumns != null && !"".equals(notNullColumns)){
                        for(String dataKey : notNullColumns.split(",")){
                            String data = (String) source.get(dataKey);
                            if(data == null) {
                                System.out.println("throw new NaviLogNullFieldException(tableType, dataKey, body)");
                            } else if ( "".equals( data ) ){
                                System.out.println("throw new NaviLogInvalidFieldException(tableType, dataKey, body)");
                            }
                        }
                    }
                }
                
                if( isCampaign ){
                    try {
                        source.put("action_body.campaign.str", mapper.writeValueAsString(campaign));    // List to JSON String
                            
                        Map<String, Object> tempMap = null;
                        Map<String, Object> campMap = null;
                        for( String campObj : campaign ){
                            campMap = makeFlatMap(mapper.readValue(campObj, Map.class), NAVI_LOG_V2_CAMPAIGN);
                            
                            tempMap = new HashMap<String, Object>();
                            tempMap.putAll(source);
                            tempMap.putAll(campMap);

                            // 캠페인 정보 필수값 체크
                            for(String campKey : "device_base_time,action_body.campaign.camp_id,action_body.campaign.camp_exec_no,action_body.campaign.chnl_node_id".split(",")){
                                String campVal = (String) tempMap.get(campKey);
                                if( campVal == null ){
                                    System.out.println("throw new NaviLogInvalidFieldException(tableType, campKey, body);");
                                } else if ( "".equals(campVal) ){
                                    System.out.println("throw new NaviLogInvalidFieldException(tableType, campKey, body);");
                                }
                            }
                        }
                        isCampaign = true;
                    } catch ( Exception e ){
                        System.out.println("throw new NaviLogInvalidFieldException(tableType, Const.NAVI_LOG_V2_CAMPAIGN, body);");
                    }
                } else {
                    isCampaign = false;
                }
            }
        } catch (Exception e1) {
            System.out.println(e1.getMessage());
        } finally {
            try {
                if (reader != null) reader.close();
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
    }
    
    private static Map<String, Object> makeFlatMap(Map<String, Object> sourceMap) throws Exception {
        return makeFlatMap(sourceMap, null);
    }
    @SuppressWarnings("unchecked")
    private static Map<String, Object> makeFlatMap(Map<String, Object> sourceMap, String prefixKey) throws Exception {
        Map<String, Object> workMap = new HashMap<String, Object>();
        
        for(String sourceKey : sourceMap.keySet()){
            Object sourceValue = (Object) sourceMap.get(sourceKey);
            String concatKey = sourceKey;
            if( prefixKey != null ){
                concatKey = Joiner.on('.').join(prefixKey, sourceKey);
            }
            
            if( sourceValue instanceof Map ){
                Map<String, Object> flatMap = makeFlatMap((Map<String, Object>) sourceValue, concatKey);
                if( flatMap != null ){
                    workMap.putAll(flatMap);
                }
                workMap.put(Joiner.on('.').join(concatKey, "str"), mapper.writeValueAsString(sourceValue));
                workMap.put(concatKey, sourceValue);
                
            } else {
                workMap.put(concatKey, sourceValue);
                System.out.println(concatKey + " : " + sourceValue);
            }
        }
        
        return workMap;
    }
}
