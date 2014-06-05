package com.stratio.streaming.commons.dto;

public class ActionCallbackDto {

    private Integer errorCode;
    private String description;

    public ActionCallbackDto() {
    }

    public ActionCallbackDto(Integer errorCode) {
        this.errorCode = errorCode;
    }

    public ActionCallbackDto(Integer errorCode, String description) {
        this.errorCode = errorCode;
        this.description = description;
    }

    public Integer getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(Integer errorCode) {
        this.errorCode = errorCode;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "ActionCallbackDto [errorCode=" + errorCode + ", description=" + description + "]";
    }

}
