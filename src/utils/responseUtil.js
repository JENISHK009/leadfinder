export const successResponse = (res, data) => {
    return res.status(200).json({
        data
    });
};


export const errorResponse = (res, message, statusCode = 400) => {
    return res.status(statusCode).json({
        message,
    });
};
