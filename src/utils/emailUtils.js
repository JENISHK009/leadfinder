import nodemailer from 'nodemailer';

if (!process.env.SMTP_HOST || !process.env.SMTP_PORT || !process.env.SMTP_EMAIL || !process.env.SMTP_PASSWORDS) {
    throw new Error('Missing required SMTP configuration in environment variables');
}

const transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: parseInt(process.env.SMTP_PORT),
    secure: false,
    auth: {
        user: process.env.SMTP_EMAIL,
        pass: process.env.SMTP_PASSWORDS,
    },
    tls: {
        rejectUnauthorized: false
    },
    connectionTimeout: 30000,
    greetingTimeout: 30000,
    socketTimeout: 30000,
    debug: true,
    logger: true
});

const commonMailOptions = {
    from: `"Exellius" <${process.env.SMTP_EMAIL}>`,
    headers: {
        'X-Mailer': 'Nodemailer',
        'X-Priority': '3',
    }
};

export async function sendOtpEmail(email, otp) {
    const mailOptions = {
        ...commonMailOptions,
        to: email,
        subject: 'Your OTP for Verification',
        text: `Your OTP for verification is: ${otp}\n\nThis OTP is valid for 10 minutes.`,
        html: `<div>Your verification code: <strong>${otp}</strong></div>`
    };

    try {
        const info = await transporter.sendMail(mailOptions);
        console.log(`OTP sent successfully to ${email}`, info.messageId);
        return info;
    } catch (error) {
        console.error('SMTP Error Details:', {
            errorCode: error.code,
            errorMessage: error.message,
            stack: error.stack
        });
        throw new Error(`Failed to send OTP email: ${error.message}`);
    }
}

export async function sendCSVEmail(toEmail, csvData, fileName = 'leads_export.csv', subject = 'Exported Leads Data', body = 'Please find the attached CSV file containing the exported leads data.') {
    try {
        const mailOptions = {
            ...commonMailOptions,
            to: toEmail,
            subject,
            text: body,
            html: `
                <div style="font-family: Arial, sans-serif; line-height: 1.6;">
                    <p>${body}</p>
                    <p>If you have any questions about this data, please reply to this email.</p>
                </div>
            `,
            attachments: [{
                filename: fileName,
                content: csvData,
                contentType: 'text/csv'
            }],
        };

        const info = await transporter.sendMail(mailOptions);
        console.log(`CSV email sent successfully to ${toEmail}`, info.messageId);
        return info;
    } catch (error) {
        console.error("Error sending CSV email:", error);
        throw new Error("Failed to send CSV email");
    }
}