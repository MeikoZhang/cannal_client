package com.sixku.canal.util;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.sixku.canal.mybatis.domain.TestCanal;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * RowData数据处理类
 * 利用反射机制，封装canal从server取过来的数据，转化成对应Mapper的实体类
 */
public class RowDataUtil {

    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 将RowData数据封装到对应实体类中
     * @param rowData   输入数据
     * @param clazz 输出Bean的类型
     * @return  输出的Bean对象
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    public static <T> T extract2Object(CanalEntry.RowData rowData, Class<T> clazz, String flag)throws Exception{

        List<CanalEntry.Column> columnsList = null;
        if("before".equals(flag)){
            columnsList = rowData.getBeforeColumnsList();
        }

        if("after".equals(flag)){
            columnsList = rowData.getAfterColumnsList();
        }

        if(columnsList == null){
            return null;
        }

        Map<String,String> rowDataMap= new HashMap<String, String>();
        for(CanalEntry.Column column : columnsList){
//            System.out.println("rowdata   " + column.getName()+" : "+column.getValue());
            rowDataMap.put(CamelUtil.underlineToCamel(column.getName()),column.getValue());
        }

        Field[] declaredFields = clazz.getDeclaredFields();
        T t = clazz.newInstance();
        for(Field field : declaredFields){

            //访问私有属性
            field.setAccessible(true);

            String rowValue = rowDataMap.get(field.getName());
            if(StringUtils.isBlank(rowValue)){
                continue;
            }

            //略过serialVersionUID
            if("serialVersionUID".equals(field.getName())){
                continue;
            }

            switch (field.getType().getSimpleName()){
               case "String":
                   field.set(t, String.valueOf(rowValue));
                   break;
                case "Integer":
                    field.set(t, Integer.valueOf(rowValue));
                    break;
                case "Long":
                    field.set(t, Long.valueOf(rowValue));
                    break;
                case "BigInteger":
                    field.set(t, BigInteger.valueOf(Long.valueOf(rowValue)));
                    break;
                case "Float":
                    field.set(t, Float.valueOf(rowValue));
                    break;
                case "Double":
                    field.set(t, Double.valueOf(rowValue));
                    break;
                case "BigDecimal":
                    field.set(t, BigDecimal.valueOf(Long.valueOf(rowValue)));
                    break;
                case "Date":
                    try {
                        System.out.println("---------date");
                        field.set(t, sdf.parse(rowValue));
                        System.out.println(sdf.parse(rowValue).toString());
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    break;
                default:
                    throw new RuntimeException("failed parse rowdata to instance.");
            }
        }
        return t;
    }

    public static void main(String[] args) throws Exception {
        Field[] declaredFields = TestCanal.class.getDeclaredFields();
        for(Field field : declaredFields){
            field.setAccessible(true);
            Class<?> type = field.getType();
//            System.out.println(field.getName() +" - "+type.getSimpleName());
        }

        System.out.println(sdf.parse("2018-11-23 13:05:01").toLocaleString());
        System.out.println(JsonUtils.toUnderlineJSONString("createTime"));
    }



}
