package com.qy.parser.bean;

import java.util.Objects;

public class EField {
    public String l;//field
    public String m;//type
    public String r;//comment

    public EField(String l, String r) {
        this.l = l;
        this.r = r;
    }

    public EField(String l, String m, String r) {
        this.l = l;
        this.m = m;
        this.r = r;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EField eField = (EField) o;
        return Objects.equals(l, eField.l) &&
                Objects.equals(m, eField.m) &&
                Objects.equals(r, eField.r);
    }

    @Override
    public int hashCode() {
        return Objects.hash(l, m, r);
    }
}