package Tool;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.PropertyUtils;

/**
 * 
 * @author Kevin 此類別主要用於轉換型態
 */
public class ETL_Tool_CastObjUtil {

	public static <T> Object[] castObjectArr(T javaBean)
			throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
		Field[] fields = javaBean.getClass().getDeclaredFields();
		Object[] objs = new Object[fields.length];

		for (int i = 0; i < fields.length; i++) {
			Field field = fields[i];
			Class<?> clazz = field.getType();

			if ("java.util.Date".equals(clazz.getName())) {
				java.util.Date utilDate = (java.util.Date) clazz
						.cast(PropertyUtils.getProperty(javaBean, field.getName()));
//				objs[i] = new java.sql.Date(utilDate.getTime());
				objs[i] = (utilDate == null) ? null : new java.sql.Date(utilDate.getTime());
			} else {

				objs[i] = clazz.cast(PropertyUtils.getProperty(javaBean, field.getName()));
		
			}
		}
		return objs;
	}

	public static <T> List<Object[]> castObjectArr(List<T> javaBeans)
			throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
		List<Object[]> list = new ArrayList<Object[]>();
		for (int i = 0; i < javaBeans.size(); i++) {
			T javaBean = javaBeans.get(i);
			Object[] objArr = ETL_Tool_CastObjUtil.castObjectArr(javaBean);
			list.add(objArr);
		}
		return list;
	}

}
