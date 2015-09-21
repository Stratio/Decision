package com.stratio.streaming.unit.siddhi.query;

/**
 * Created by aitor on 9/18/15.
 */
public abstract class OrdersQueries {

    public static final String STREAM_ORDERS= "c_orders";

    public static final String STREAM_LINES= "c_lines";

    public static final String STREAM_FRAUD= "c_fraud";

    public static final String STREAM_ALERTS= "c_alerts";

    public static final String QUERY_CREATE_STREAM_ORDERS=  "define stream " + STREAM_ORDERS + " (" +
            "order_id string, timestamp string, day_time_zone string, client_id int," +
            "payment_method string, latitude double, longitude double, credit_card string, " +
            "shopping_center string, channel string, city string, country string, employee int, " +
            "total_amount float, total_products int, order_size string, lines string); ";

    public static final String QUERY_CREATE_STREAM_LINES=  "define stream " + STREAM_LINES + " (" +
            "order_id string, timestamp string, day_time_zone string, client_id int, " +
            "payment_method string, latitude double, longitude double, credit_card string, " +
            "shopping_center string, channel string, city string, country string, " +
            "employee int, total_amount float, total_products int, order_size string, " +
            "product string, family string, quantity int, price float); ";


    /**
     * Orders Queries
     */
    public static final String QUERY_MORE_1ORDER_IN_5M = "from every(o1 = " + STREAM_ORDERS + " -> o2 = " + STREAM_ORDERS +
            "[client_id==o1.client_id and shopping_center != o1.shopping_center]) within 300000 " +
            "select o1.order_id, o1.client_id, o1.latitude, o1.longitude, o1.credit_card, o1.shopping_center, 'orders-1' as origin, " +
            "'Fraud: More than one order in less than 5 minutes in different shopping centers' as reason " +
            "insert into " + STREAM_FRAUD;

    public static final String QUERY_MORE_1ORDER_IN_5M_SAME_CARD = "from every(o1 = " + STREAM_ORDERS +
            " -> o2 = " + STREAM_ORDERS + "[client_id==o1.client_id and credit_card==o1.credit_card and city != o1.city]) " +
            "within 300000 " +
            "select o1.order_id, o1.client_id, o1.latitude, o1.longitude, o1.credit_card, o1.shopping_center, 'orders-2' as origin, " +
            "'Fraud: More than one order in less than 5 minutes with the same credit card in different cities' " +
            "as reason insert into " + STREAM_FRAUD;

    public static final String QUERY_MORE_1_ORDER_IN_10M = "from every(o1 = " + STREAM_ORDERS + " -> o2 = " +
            STREAM_ORDERS + "[client_id==o1.client_id and credit_card != o1.credit_card]) within 600000 " +
            "select o1.order_id, o1.client_id, o1.latitude, o1.longitude, o1.credit_card, o1.shopping_center, 'orders-3' as origin, " +
            "'Fraud: More than one order with different credit cards in less than 10 minutes' as reason " +
            "insert into " + STREAM_FRAUD;

    public static final String QUERY_MORE_2_SALES_10P_IN_20M = "from every(o1 = " + STREAM_ORDERS + " -> o2 = " +
            STREAM_ORDERS + "[client_id == o1.client_id " +
            "and o1.client_id == o2.client_id and client_id == o2.client_id and total_products > 10 " +
            "and o1.total_products > 10 and o2.total_products > 10]) within 1200000 " +
            "select o1.order_id, o1.client_id, o1.latitude, o1.longitude, o1.credit_card, o1.shopping_center,  'orders-4' as origin, " +
            "'Fraud: More than 2 sales with total_products higher than 10  in less than 20 minutes' as reason " +
            "insert into " + STREAM_FRAUD;

    /**
     * Lines Queries
     */
    public static final String QUERY_MORE_3SALES_BELOW20_10MIN = "from every(o1 = " + STREAM_LINES + " -> o2 = " + STREAM_LINES + " -> o3 = " +
            STREAM_LINES + "[client_id == o1.client_id and o1.client_id == o2.client_id and " +
            "client_id == o2.client_id and total_amount < 20 and o1.total_amount < 20 " +
            "and o2.total_amount <20]) within 600000 " +
            "select o1.order_id, o1.client_id, o1.latitude, o1.longitude, o1.credit_card, o1.shopping_center, 'lines-1' as origin, " +
            "'Fraud: More than 3 sales with total amount below 20€ in less than 10 minutes' as reason " +
            "insert into " + STREAM_FRAUD;

    public static final String QUERY_AVG_QUANTITY_HIGHER_15P_10MIN = "from " + STREAM_LINES + "#window.time( 10 min ) " +
            "select order_id, longitude, latitude, shopping_center, product, quantity, 'lines-2' as origin, " +
            "avg(quantity) as avgQuantityTotal group by product having quantity>1.15*avgQuantityTotal  " +
            "insert into " + STREAM_ALERTS;


}
