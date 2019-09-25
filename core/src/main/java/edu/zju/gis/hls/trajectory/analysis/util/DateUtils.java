package edu.zju.gis.hls.trajectory.analysis.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class DateUtils extends Date {

    /**
     * 日期的方法
     */
    private static final long serialVersionUID = 1L;
    private static int year = 2014;
    private static int month = 0;
    private static int day = 1;
    private static int hour = 0;
    private static int min = 0;
    private static int sec = 0;
    private static int millsec = 0;

    public static Date getTime(Date date) throws ParseException {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        year = cal.get(Calendar.YEAR);
        month = cal.get(Calendar.MONTH);
        day = cal.get(Calendar.DATE);
        hour = cal.get(Calendar.HOUR);
        min = cal.get(Calendar.MINUTE);
        sec = cal.get(Calendar.SECOND);
        millsec = cal.get(Calendar.MILLISECOND);
        Date newdate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(year + "-" + month + "-" + day + " " + hour
                + ":" + min + ":" + sec + "." + millsec);
        return newdate;
    }

    public static int getHour(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    /**
     * @return 当前时间
     */
    public static Date now() {
        Date date = new Date();
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
        return date;
    }

    /**
     * 根据毫秒字符串获取date
     * @param msStr
     * @return
     */
    public static Date getDateMS(String msStr) {
        return new Date(Long.valueOf(msStr));
    }

    /**
     * 当天零点
     */
    public static Date beginOfDay(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Date newdate = cal.getTime();
        return newdate;
    }

    /**
     * @param date 起始时间
     * @param days 需要增减的天数
     * @return 天数增减后的时间
     */
    public static Date addDays(Date date, int days) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, days);

        Date newdate = cal.getTime();
        return newdate;
    }

    public static Date addMinutes(Date date, int minutes) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MINUTE, minutes);

        Date newdate = cal.getTime();
        return newdate;
    }

    /**
     * @param date  起始时间
     * @param hours 需要增减的小时数
     * @return 天数增减后的时间
     */
    public static Date addHours(Date date, int hours) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.HOUR, hours);
        Date newdate = cal.getTime();
        return newdate;
    }

    public static Date addMonths(Date date, int amounts) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, amounts);
        Date newdate = cal.getTime();
        return newdate;
    }

    /**
     * @param date 输入的时间
     * @return 输入时间的前一天晚上23：59：59
     */
    public static Date endOfYesterday(Date date) {
        Date begin = beginOfDay(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(begin);
        cal.add(Calendar.SECOND, -1);

        Date newdate = cal.getTime();
        return newdate;
    }

    /**
     * @param date
     * @return 输入日期的当月1日凌晨
     */
    public static Date firstDayOfMonth(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.set(date.getYear() + 1900, date.getMonth(), 1, 0, 0, 0);
        return cal.getTime();
    }

    /**
     * @param date
     * @return 输入日期的上月最后一天23：59：59
     */
    public static Date lastDayOfLastMonth(Date date) {
        return endOfYesterday(firstDayOfMonth(date));
    }

    /**
     * @param date
     * @return 当年一月一日零点
     */
    public static Date firstDayOfYear(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.set(date.getYear() + 1900, 0, 1, 0, 0, 0);
        return cal.getTime();
    }

    /**
     * @param date
     * @param amount 月份的增减
     * @return N月前（后）的1日零点
     */
    public static Date firstDayOfNMonthsBefore(Date date, int amount) {
        Calendar cal = Calendar.getInstance();
        Date addmonth = addMonths(date, amount);
        cal.set(addmonth.getYear() + 1900, addmonth.getMonth(), 1, 0, 0, 0);
        return cal.getTime();
    }

    /**
     * @param date
     * @param amount 月份的增减
     * @return N月前（后）末的23：59：59
     */
    public static Date lastDayOfNMonthsBefore(Date date, int amount) {
        return endOfYesterday(firstDayOfNMonthsBefore(date, amount + 1));
    }

    /**
     * 上年年末
     *
     * @param date
     * @return
     */
    public static Date lastDayOfLastYear(Date date) {
        Calendar cal = Calendar.getInstance();
        Date firstdayofyear = firstDayOfYear(date);
        cal.setTime(firstdayofyear);
        cal.add(Calendar.MILLISECOND, -1);
        return cal.getTime();
    }


    public static Date beginOfTomorrow(Date date) {
        return beginOfDay(addDays(date, 1));
    }


    public static String getLastDayOfMonth(Date date) {
        SimpleDateFormat simpleDateFormat = getDateParser("yyyy-MM-dd");
        String today = simpleDateFormat.format(date);
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, Integer.parseInt(today.split("-")[0]));
        cal.set(Calendar.MONTH, Integer.parseInt(today.split("-")[1]) - 1);
        int lastDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
        cal.set(Calendar.DAY_OF_MONTH, lastDay);

        String lastDayOfMonth = simpleDateFormat.format(cal.getTime());

        return lastDayOfMonth;
    }

    public static Date endDayOfMonth(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 59);
        return cal.getTime();
    }

    /**
     * N个月的同一时间
     *
     * @param date
     * @return
     */
    public static Date getDayOfMonthsBefore(Date date, int month) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, -month);
        return cal.getTime();
    }

    public static SimpleDateFormat getDateParser(String pattern) {
        return new SimpleDateFormat(pattern);
    }

    public static SimpleDateFormat getDateParserUS(String pattern) {
        return new SimpleDateFormat(pattern, Locale.US);
    }

    public static SimpleDateFormat getGenenalParser() {
        return getDateParser("yyyy-MM-dd HH:mm:ss");
    }

    public static SimpleDateFormat getGenenalParserUS() {
        return getDateParserUS("EEE MMM dd HH:mm:ss Z yyyy");
    }

    public static String getGenenalDate(Date date) {
        return getGenenalParser().format(date);
    }

    public static Date genenalDateParse(String date) {
        try {
            return getGenenalParser().parse(date);
        } catch (ParseException e) {
            throw new RuntimeException("日期解析异常");
        }
    }

    public static String getGenenalDateMillis(Date date) {
        return new SimpleDateFormat("yyMMddHHmmssSSS").format(date);
    }

    /**
     * @return 返回时间yyyyMMddHHmmss 14位形式
     */
    public static String curDateTimeStr14() {
        Date date = new Date();
        return getDateParser("yyyyMMddHHmmss").format(date);
    }

    /**
     * @return 返回时间yyyy-MM-dd HH:mm:ss
     */
    public static String curDateTimeStrDefault() {
        Date date = new Date();
        return getDateParser("yyyy-MM-dd HH:mm:ss").format(date);
    }


    public static String format(String format, Date date) {
        return getDateParser(format).format(date);
    }

    /**
     * @return 返回时间yyyy-MM-dd 8位形式
     */
    public static String curDateToDay() {
        Date date = new Date();
        return getDateParser("yyyyMMdd").format(date);
    }

    public static Date endOfDate(String dateTime) {
        try {
            return DateUtils.endOfYesterday(DateUtils.addDays(getDateParser("yyyy-MM-dd").parse(dateTime), 1));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Date getYesterday() {
        return addDays(new Date(), -1);
    }

    /**
     * 返回 yyyy-MM-dd 的SimpleDateFormat
     *
     * @return
     */
    public static SimpleDateFormat YYYY_MM_DD() {
        return getDateParser("yyyy-MM-dd");
    }

    /**
     * 返回yyyy-MM 的SimpleDateFormat
     *
     * @return
     */
    public static SimpleDateFormat YYYY_MM() {
        return getDateParser("yyyy-MM");
    }

    public static int daysBetween(Date smdate, Date bdate) {
        long intervalMilli = smdate.getTime() - bdate.getTime();

        return (int) (intervalMilli / (24 * 60 * 60 * 1000));
    }

    /**
     * 返回两个时间相隔的天数
     */
    public static float getDaysByCompare(String preDatetime, String afterDatetime, Boolean hasFloat) {
        return getByCompare(preDatetime, afterDatetime, 1, hasFloat);
    }

    /**
     * 返回两个时间相隔的小时数
     */
    public static float getHoursByCompare(String preDatetime, String afterDatetime, Boolean hasFloat) {
        return getByCompare(preDatetime, afterDatetime, 2, hasFloat);
    }

    /**
     * 返回两个时间相隔的分钟数
     */
    public static float getMinutesByCompare(String preDatetime, String afterDatetime, Boolean hasFloat) {
        return getByCompare(preDatetime, afterDatetime, 3, hasFloat);
    }

    /**
     * 返回两个时间相隔的秒数
     */
    public static float getSecondsByCompare(String preDatetime, String afterDatetime, Boolean hasFloat) {
        return getByCompare(preDatetime, afterDatetime, 4, hasFloat);
    }

    public static float getByCompare(String preDatetime, String afterDatetime,
                                     Integer type, Boolean hasFloat) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date preD = null;
        Date afterD = null;
        try {
            preD = df.parse(preDatetime);
            afterD = df.parse(afterDatetime);
        } catch (Exception e) {
        }
        return getTypeByCompare(preD, afterD, type, hasFloat);
    }

    /**
     * 获得当前日期是星期几
     *
     * @return { "星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六" }
     */
    public static String curWeekDay() {
        Date date = new Date();
        SimpleDateFormat dateFm = new SimpleDateFormat("EEEE");
        return dateFm.format(date);
    }

    public static float getSecondsByCompare(Date preDatetime, Date afterDatetime, Boolean hasFloat) {
        return getTypeByCompare(preDatetime, afterDatetime, 4, hasFloat);
    }

    public static float getTypeByCompare(Date preDate, Date afterDate, Integer type, Boolean hasFloat) {
        if (preDate == null || afterDate == null) {
            throw new RuntimeException(String.format("比较时间函数参数缺失, %s, %s", DateUtils.getGenenalDate(preDate), DateUtils.getGenenalDate(afterDate)));
        }
        long diff = afterDate.getTime() - preDate.getTime();
        long l = 1;
        switch (type) {
            case 1:
                l = 1000 * 60 * 60 * 24;
                break;
            case 2:
                l = 1000 * 60 * 60;
                break;
            case 3:
                l = 1000 * 60;
                break;
            case 4:
                l = 1000;
                break;
        }
        if (hasFloat) {
            float a = diff;
            float b = l;
            return a / b;
        } else {
            return diff / l;
        }
    }
}
