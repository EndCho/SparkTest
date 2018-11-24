package cn.rzlee.spark.core;

import scala.math.Ordered;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Author ^_^
 * @Create 2018/11/3
 * 自定义的二次排序key
 *
 */
public class SecondarySortByKey implements Ordered<SecondarySortByKey>, Serializable {
    // 首先在自定义的key里面，定义需要进行排序的列

    private int first;
    private int second;


    public SecondarySortByKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SecondarySortByKey that = (SecondarySortByKey) o;
        return first == that.first &&
                second == that.second;
    }

    @Override
    public int hashCode() {

        return Objects.hash(first, second);
    }

    @Override
    public int compare(SecondarySortByKey that) {
        return 0;
    }

    @Override
    public boolean $less(SecondarySortByKey other) {
        if (this.$less(other)){
            return true;
        }else if (this.getFirst()== other.getFirst() && this.getSecond()< other.getSecond()){
            return true;
        }

        return false;
    }

    @Override
    public boolean $greater(SecondarySortByKey other) {
        if (this.getFirst()>other.getFirst()){
            return true;
        }else if (this.getFirst() == other.getFirst() && this.getSecond() > other.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortByKey other) {
        if (this.$less(other)){
            return true;
        }else if (this.getFirst()== other.getFirst() && this.getSecond() == other.getSecond()){
            return true;
        }

        return false;
    }

    @Override
    public boolean $greater$eq(SecondarySortByKey other) {
        if (this.$greater(other)){
            return true;
        }else if(this.getFirst() == other.getFirst() && this.getSecond() == other.getSecond()){
            return true;
        }

        return false;
    }

    @Override
    public int compareTo(SecondarySortByKey other) {
        if (this.first - other.first !=0){
            return this.first - other.getFirst();
        }else{
            return this.second - other.getSecond();
        }
    }


    // 为要进行排序的多个列，提供getter和setter方法，以及hashCode和equals方法
    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }



}
