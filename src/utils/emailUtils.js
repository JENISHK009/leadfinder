import nodemailer from 'nodemailer';

const transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: process.env.SMTP_PORT,
    secure: false,
    auth: {
        user: process.env.SMTP_EMAIL,
        pass: process.env.SMTP_PASSWORDS,
    },
});

export async function sendOtpEmail(email, otp) {
    const mailOptions = {
        from: 'youremail@gmail.com',
        to: email,
        subject: 'Your OTP for Verification',
        text: `Your OTP for verification is: ${otp}`,
    };

    try {
        await transporter.sendMail(mailOptions);
        console.log(`OTP sent to ${email}`);
    } catch (error) {
        console.error('Error sending OTP email:', error);
    }
}

export async function sendCSVEmail(toEmail, csvData, fileName = 'leads_export.csv', subject = 'Exported Leads Data', body = 'Please find the attached CSV file containing the exported leads data.') {
    try {
        const mailOptions = {
            from: process.env.EMAIL_USER,
            to: toEmail,
            subject,
            text: body,
            attachments: [{ filename: fileName, content: csvData }],
        };

        await transporter.sendMail(mailOptions);
        console.log(`CSV email sent successfully to ${toEmail}`);
    } catch (error) {
        console.error("Error sending CSV email:", error);
        throw new Error("Failed to send CSV email");
    }
}
